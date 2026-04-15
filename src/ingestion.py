"""
kafka_s3_complete.py
====================
Single-file Kafka -> S3 Bronze Layer Pipeline for Market Risk Platform

Modes:
    python kafka_s3_complete.py produce          # generate & send mock market risk events
    python kafka_s3_complete.py consume          # consume and write to S3 bronze layer
    python kafka_s3_complete.py produce --count 100000
    python kafka_s3_complete.py consume --group my-group

Topics:
    exposure      -> s3://mr-risk-platform/bronze/raw/exposure/
    market_prices -> s3://mr-risk-platform/bronze/raw/market_prices/

S3 layout (Hive-partitioned, Glue/Athena ready):
    s3://mr-risk-platform/
      bronze/
        raw/
          exposure/
            year=2026/month=04/day=13/hour=19/
              exposure_p0_o0-4999_1744584000000.parquet
          market_prices/
            year=2026/month=04/day=13/hour=19/
              market_prices_p1_o0-4999_1744584000000.parquet
        dlq/
          exposure/   <- bad messages land here as JSON
          market_prices/

Environment variables:
    KAFKA_BOOTSTRAP          Confluent Cloud broker (required)
    KAFKA_API_KEY            Confluent Cloud API key (required)
    KAFKA_API_SECRET         Confluent Cloud API secret (required)
    AWS_ACCESS_KEY_ID        AWS credentials (or use IAM role)
    AWS_SECRET_ACCESS_KEY    AWS credentials (or use IAM role)
    AWS_DEFAULT_REGION       Default: us-east-1
    KAFKA_GROUP_ID           Default: mr-risk-s3-sink
    BATCH_SIZE               Records per Parquet file. Default: 5000
    FLUSH_INTERVAL_SECS      Max age before forced flush. Default: 60
    ENV                      dev | staging | prod. Default: dev

Install:
    pip install confluent-kafka pyarrow boto3

"""

import argparse
import io
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Event, Lock
from typing import Any, Callable, Dict, List, Optional, Tuple

# ── Third-party ──────────────────────────────────────────────────────────────
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("kafka.pipeline")

# ── Topic -> S3 routing ───────────────────────────────────────────────────────
TOPIC_ROUTING: Dict[str, Dict[str, str]] = {
    "exposure": {
        "bucket": "mr-risk-platform",
        "prefix": "bronze/raw/exposure",
    },
    "market_prices": {
        "bucket": "mr-risk-platform",
        "prefix": "bronze/raw/market_prices",
    },
}

# ── Bronze Parquet schema ─────────────────────────────────────────────────────
BRONZE_SCHEMA = pa.schema([
    pa.field("kafka_topic",     pa.string(),                  nullable=False),
    pa.field("kafka_partition", pa.int32(),                   nullable=False),
    pa.field("kafka_offset",    pa.int64(),                   nullable=False),
    pa.field("kafka_timestamp", pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("kafka_key",       pa.string(),                  nullable=True),
    pa.field("kafka_headers",   pa.string(),                  nullable=True),
    pa.field("payload",         pa.string(),                  nullable=False),
    pa.field("ingested_at",     pa.timestamp("ms", tz="UTC"), nullable=False),
])

# =============================================================================
# SECTION 1: S3 PARQUET SINK
# =============================================================================

@dataclass
class PartitionBuffer:
    topic:        str   = ""
    partition:    int   = -1
    records:      List  = field(default_factory=list)
    start_offset: int   = -1
    end_offset:   int   = -1
    created_at:   float = field(default_factory=time.monotonic)

    def add(self, record: dict, offset: int) -> None:
        self.records.append(record)
        if self.start_offset == -1:
            self.start_offset = offset
        self.end_offset = offset

    @property
    def age_secs(self) -> float:
        return time.monotonic() - self.created_at

    @property
    def size(self) -> int:
        return len(self.records)

    def clear(self) -> None:
        self.records      = []
        self.start_offset = -1
        self.end_offset   = -1
        self.created_at   = time.monotonic()


class S3ParquetSink:
    """
    Buffers Kafka messages per (topic, partition).
    Flushes to S3 as Parquet when:
      - Buffer count >= batch_size  (count-triggered flush)
      - Buffer age   >= flush_interval_secs  (time-triggered flush)

    File naming embeds offset range for full lineage:
      exposure_p0_o0-4999_1744584000000.parquet
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        batch_size: int          = 5_000,
        flush_interval_secs: float = 60.0,
        aws_region: Optional[str]  = None,
    ):
        self.bucket              = bucket
        self.prefix              = prefix.rstrip("/")
        self.batch_size          = batch_size
        self.flush_interval_secs = flush_interval_secs

        self._buffers:  Dict[Tuple[str, int], PartitionBuffer] = defaultdict(PartitionBuffer)
        self._lock      = Lock()
        self._files_written   = 0
        self._records_written = 0
        self._bytes_written   = 0
        self._flush_errors    = 0

        self._s3 = boto3.client(
            "s3",
            region_name=aws_region or os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            config=BotoConfig(
                retries={"max_attempts": 5, "mode": "adaptive"},
                max_pool_connections=20,
            ),
        )
        logger.info(
            "S3ParquetSink | s3://%s/%s | batch=%d interval=%ds",
            bucket, prefix, batch_size, flush_interval_secs,
        )

    # ── Public ────────────────────────────────────────────────────────────────

    def add(
        self,
        topic: str,
        partition: int,
        offset: int,
        kafka_timestamp_ms: Optional[int],
        key: Optional[str],
        headers: Optional[Dict[str, str]],
        payload: Any,
    ) -> bool:
        """Buffer one message. Returns True if a count-flush was triggered."""
        record = {
            "kafka_topic":     topic,
            "kafka_partition": partition,
            "kafka_offset":    offset,
            "kafka_timestamp": (
                datetime.fromtimestamp(kafka_timestamp_ms / 1000, tz=timezone.utc)
                if kafka_timestamp_ms else None
            ),
            "kafka_key":       key,
            "kafka_headers":   json.dumps(headers) if headers else None,
            "payload":         payload if isinstance(payload, str)
                               else json.dumps(payload, default=str),
            "ingested_at":     datetime.now(tz=timezone.utc),
        }
        key_tp        = (topic, partition)
        flush_triggered = False

        with self._lock:
            buf = self._buffers[key_tp]
            if buf.topic == "":
                buf.topic     = topic
                buf.partition = partition
            buf.add(record, offset)
            if buf.size >= self.batch_size:
                self._flush_buffer(key_tp)
                flush_triggered = True
        return flush_triggered

    def maybe_flush_by_age(self) -> int:
        """Flush all buffers older than flush_interval_secs. Returns count flushed."""
        flushed = 0
        with self._lock:
            for key_tp, buf in list(self._buffers.items()):
                if buf.size > 0 and buf.age_secs >= self.flush_interval_secs:
                    self._flush_buffer(key_tp)
                    flushed += 1
        return flushed

    def flush_partition(self, topic: str, partition: int) -> bool:
        """Flush a specific partition — call on rebalance revoke."""
        key_tp = (topic, partition)
        with self._lock:
            buf = self._buffers.get(key_tp)
            if buf and buf.size > 0:
                self._flush_buffer(key_tp)
                return True
        return False

    def flush_all(self) -> int:
        """Flush everything. Call on shutdown."""
        flushed = 0
        with self._lock:
            for key_tp, buf in list(self._buffers.items()):
                if buf.size > 0:
                    self._flush_buffer(key_tp)
                    flushed += 1
        return flushed

    @property
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            buffered = sum(b.size for b in self._buffers.values())
        return {
            "files_written":    self._files_written,
            "records_written":  self._records_written,
            "bytes_written_mb": round(self._bytes_written / (1024 ** 2), 2),
            "flush_errors":     self._flush_errors,
            "buffered_records": buffered,
            "s3_path":          f"s3://{self.bucket}/{self.prefix}",
        }

    # ── Private ───────────────────────────────────────────────────────────────

    def _flush_buffer(self, key_tp: Tuple[str, int]) -> None:
        """Serialize to Parquet bytes and PUT to S3. Must be called under lock."""
        buf = self._buffers[key_tp]
        if not buf.records:
            return

        records      = list(buf.records)
        start_offset = buf.start_offset
        end_offset   = buf.end_offset
        topic        = buf.topic
        partition    = buf.partition
        buf.clear()

        now     = datetime.now(tz=timezone.utc)
        s3_key  = self._build_s3_key(topic, partition, start_offset, end_offset, now)

        # Release lock during I/O — buffer is already cleared
        self._lock.release()
        try:
            parquet_bytes = self._to_parquet(records)
            self._put_s3(s3_key, parquet_bytes)
            self._files_written   += 1
            self._records_written += len(records)
          
