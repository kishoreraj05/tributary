"""Automatic Spark sizing.

Layers, most specific wins: Spark's own adaptivity (AQE + dynamic allocation) is always
on; ``resources: auto`` picks a t-shirt profile from estimated input size and pipeline
shape; explicit ``resources`` fields and raw ``spark_conf`` override both.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field

from ..spec import CompiledPipeline
from ..spec.models import ResourceSettings

GB = 1024**3
PARTITION_BYTES = 128 * 1024**2

PROFILES: dict[str, dict] = {
    "S": {"executors": (1, 2), "executor_cores": 2, "executor_memory": "4g", "driver_memory": "2g"},
    "M": {"executors": (2, 10), "executor_cores": 4, "executor_memory": "8g", "driver_memory": "4g"},
    "L": {"executors": (4, 40), "executor_cores": 4, "executor_memory": "16g", "driver_memory": "8g"},
    "XL": {"executors": (10, 100), "executor_cores": 5, "executor_memory": "24g", "driver_memory": "16g"},
}
_ORDER = ["S", "M", "L", "XL"]
_THRESHOLDS = [(1 * GB, "S"), (20 * GB, "M"), (200 * GB, "L")]

_ADAPTIVE = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
}


@dataclass
class ResourceDecision:
    master: str | None
    profile: str
    reasons: list[str] = field(default_factory=list)
    conf: dict[str, str] = field(default_factory=dict)
    input_bytes: int | None = None

    def describe(self) -> str:
        size = "unknown" if self.input_bytes is None else f"{self.input_bytes / GB:.2f} GB"
        lines = [f"Resources: profile={self.profile} master={self.master or '(from spark-submit)'} input={size}"]
        lines += [f"  - {r}" for r in self.reasons]
        lines += [f"  {k}={v}" for k, v in sorted(self.conf.items())]
        return "\n".join(lines)


def estimate_input(compiled: CompiledPipeline) -> tuple[int | None, int]:
    """(total known input bytes or None if any source is unknown, complexity score)."""
    registry, spec = compiled.registry, compiled.spec
    total, unknown = 0, False
    for s in spec.sources:
        size = registry.create("source", s.type).estimate_input_bytes(s.options)
        if size is None:
            unknown = True
        else:
            total += size
    complexity = sum(registry.create("transform", t.type).complexity_hint(t.options) for t in spec.transforms)
    return (None if unknown else total), complexity


def decide_resources(compiled: CompiledPipeline) -> ResourceDecision:
    runtime = compiled.runtime
    # Under spark-submit the master and deploy mode are already decided.
    master = None if os.environ.get("PYSPARK_GATEWAY_PORT") else (runtime.master or "local[*]")
    local = bool(master and master.startswith("local"))
    explicit = runtime.resources if isinstance(runtime.resources, ResourceSettings) else ResourceSettings()
    size, complexity = estimate_input(compiled)
    reasons: list[str] = []

    if explicit.profile != "auto":
        profile = explicit.profile
        reasons.append("profile set explicitly")
    else:
        profile = _auto_profile(size, complexity, compiled.mode, reasons)

    conf = dict(_ADAPTIVE)
    conf["spark.sql.shuffle.partitions"] = str(_shuffle_partitions(size, local))
    if local:
        reasons.append("local master: executor settings do not apply; using local cores")
        if explicit.driver_memory:
            conf["spark.driver.memory"] = explicit.driver_memory
    else:
        p = PROFILES[profile]
        low, high = p["executors"]
        low = explicit.min_executors if explicit.min_executors is not None else low
        high = explicit.max_executors or high
        low = min(low, high)
        conf.update(
            {
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                "spark.dynamicAllocation.minExecutors": str(low),
                "spark.dynamicAllocation.initialExecutors": str(low),
                "spark.dynamicAllocation.maxExecutors": str(high),
                "spark.executor.cores": str(explicit.executor_cores or p["executor_cores"]),
                "spark.executor.memory": explicit.executor_memory or p["executor_memory"],
                "spark.driver.memory": explicit.driver_memory or p["driver_memory"],
            }
        )
    if compiled.spec.execution.parallel_sinks:
        conf["spark.scheduler.mode"] = "FAIR"
    if runtime.spark_conf:
        reasons.append(f"{len(runtime.spark_conf)} spark_conf override(s) applied last")
        conf.update({k: (str(v).lower() if isinstance(v, bool) else str(v)) for k, v in runtime.spark_conf.items()})
    return ResourceDecision(master=master, profile=profile, reasons=reasons, conf=conf, input_bytes=size)


def _auto_profile(size: int | None, complexity: int, mode: str, reasons: list[str]) -> str:
    if size is None:
        reasons.append("input size unknown (remote or streaming source); defaulting to M")
        profile = "M"
    else:
        profile = next((name for limit, name in _THRESHOLDS if size < limit), "XL")
        reasons.append(f"estimated input {size / GB:.2f} GB -> {profile}")
    if complexity >= 3 and profile != "XL":
        profile = _ORDER[_ORDER.index(profile) + 1]
        reasons.append(f"{complexity} joins/aggregations -> bumped to {profile}")
    return profile


def _shuffle_partitions(size: int | None, local: bool) -> int:
    if local:
        cores = os.cpu_count() or 4
        if size is None:
            return max(4, cores * 2)
        return max(4, min(cores * 2, math.ceil(size / PARTITION_BYTES)))
    if size is None:
        return 200
    return max(16, min(2000, math.ceil(size / PARTITION_BYTES)))
