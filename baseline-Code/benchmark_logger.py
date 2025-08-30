# benchmark_logger.py
# Robust interval-based metrics logger for the real-time detector.
# Python 3.9 compatible.

import time
import csv
import json
import os
import atexit
import uuid
from typing import Optional, Dict, Any, Callable, List
from collections import defaultdict

# Optional system metrics
try:
    import psutil
    _HAS_PSUTIL = True
except Exception:
    _HAS_PSUTIL = False


def _iso_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _percentile(sorted_vals: List[float], pct: float) -> float:
    """pct in [0,100]; returns percentile or 0 if empty."""
    if not sorted_vals:
        return 0.0
    n = len(sorted_vals)
    if n == 1:
        return float(sorted_vals[0])
    k = (n - 1) * (pct / 100.0)
    f = int(k)
    c = f + 1
    if c >= n:
        return float(sorted_vals[-1])
    return float(sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f))


class BenchmarkLogger:
    """
    Interval-based metrics logger.

    Cumulative columns (kept compatible with your previous CSV intent):
      - timestamp
      - total_trades
      - total_anomalies
      - detection_rate_pct
      - avg_processing_time_sec
      - avg_detection_time_sec
      - total_alerts
      - memory_mb
      - cpu_pct

    New columns (per your schema extension):
      - run_id, env_mode, replica, partitions
      - e2e_p50_ms, e2e_p95_ms, e2e_p99_ms, e2e_max_ms
      - lag_min, lag_avg, lag_max
      - refits, refit_time_ms
      - io_write_ms_avg, bytes_written
      - top_symbols_json
    """

    def __init__(
        self,
        csv_path: str = "benchmark.csv",
        json_path: str = "benchmark_summary.json",
        report_interval_sec: int = 10,
        run_metadata: Optional[Dict[str, Any]] = None,
        start_time_monotonic: Optional[float] = None,
        # Legacy alias (supported for flexibility)
        report_interval: Optional[int] = None,
        # Future-proof: ignore unexpected kwargs
        **_ignored: Any,
    ):
        # Support legacy arg if provided
        if report_interval is not None and (report_interval_sec is None or report_interval_sec == 10):
            report_interval_sec = int(report_interval)

        self.csv_path = csv_path
        self.json_path = json_path
        self.report_interval_sec = max(1, int(report_interval_sec))

        # Monotonic timing base
        self._t0 = start_time_monotonic if start_time_monotonic is not None else time.monotonic()
        self._last_flush = self._t0

        # Cumulative counters
        self.total_trades = 0
        self.total_anomalies = 0
        self.total_alerts = 0

        # Interval sums
        self._sum_processing_time = 0.0
        self._sum_detection_time = 0.0

        # E2E latency (ms)
        self._e2e_ms: List[float] = []
        self._e2e_ms_per_symbol: Dict[str, List[float]] = defaultdict(list)

        # Per-symbol tallies (cumulative)
        self._trades_per_symbol: Dict[str, int] = defaultdict(int)
        self._anoms_per_symbol: Dict[str, int] = defaultdict(int)

        # Kafka lag snapshots (interval)
        self._lag_min_samples: List[int] = []
        self._lag_avg_samples: List[float] = []
        self._lag_max_samples: List[int] = []

        # Refit accounting (interval)
        self._refits = 0
        self._refit_time_ms_sum = 0.0

        # Sink I/O (interval)
        self._io_write_ms_samples: List[float] = []
        self._bytes_written_sum = 0

        # Exceptions (cumulative count)
        self._exceptions = 0

        # Lifecycle
        self._first_decision_marked = False
        self._startup_to_first_decision_ms: Optional[float] = None

        # Run metadata
        md = {
            "run_id": str(uuid.uuid4()),
            "env_mode": "Local",
            "region": "",
            "replica": "",
            "partitions": 0,
        }
        if run_metadata:
            # Only copy known keys or any non-None values
            for k, v in run_metadata.items():
                if v is not None:
                    md[k] = v
        self.run_metadata = md

        # Prepare CSV header fresh each run (clean experiments)
        self._init_csv()

        # Flush on exit
        atexit.register(self.shutdown)

    # -----------------------
    # Public logging methods
    # -----------------------
    def log_trade(self, symbol: str, processing_time_sec: float, detection_time_sec: float) -> None:
        self.total_trades += 1
        self._sum_processing_time += float(processing_time_sec or 0.0)
        self._sum_detection_time += float(detection_time_sec or 0.0)
        if symbol:
            self._trades_per_symbol[symbol] += 1

    def log_anomaly(self, symbol: Optional[str] = None) -> None:
        self.total_anomalies += 1
        if symbol:
            self._anoms_per_symbol[symbol] += 1

    def log_e2e_ms(self, symbol: Optional[str], e2e_ms: float) -> None:
        val = float(e2e_ms or 0.0)
        if val < 0:
            val = 0.0
        self._e2e_ms.append(val)
        if symbol:
            self._e2e_ms_per_symbol[symbol].append(val)

    def log_io(self, write_ms: float, bytes_written: int) -> None:
        try:
            if write_ms is not None:
                self._io_write_ms_samples.append(float(write_ms))
            if bytes_written is not None:
                self._bytes_written_sum += int(bytes_written)
        except Exception:
            # Never fail caller due to metrics
            pass

    def log_refit(self, duration_ms: float, symbol: Optional[str] = None) -> None:
        self._refits += 1
        self._refit_time_ms_sum += float(duration_ms or 0.0)

    def log_lag(self, min_lag: int, avg_lag: float, max_lag: int) -> None:
        self._lag_min_samples.append(int(min_lag or 0))
        self._lag_avg_samples.append(float(avg_lag or 0.0))
        self._lag_max_samples.append(int(max_lag or 0))

    def inc_alerts(self, n: int = 1) -> None:
        self.total_alerts += int(n or 0)

    def inc_exceptions(self, n: int = 1) -> None:
        self._exceptions += int(n or 0)

    def set_partitions(self, n: int) -> None:
        self.run_metadata["partitions"] = int(n)

    def set_replica(self, r: Any) -> None:
        self.run_metadata["replica"] = str(r)

    def mark_first_decision(self) -> None:
        if not self._first_decision_marked:
            self._first_decision_marked = True
            self._startup_to_first_decision_ms = (time.monotonic() - self._t0) * 1000.0

    # Legacy convenience (safe no-op if older code uses it)
    def log_event(self, event_type: str, trade_id: Any = None, duration: Any = None, symbol: Optional[str] = None) -> None:
        if event_type == 'trade':
            self.log_trade(symbol or "", float(duration or 0.0), 0.0)
        elif event_type == 'detection':
            self.log_trade(symbol or "", 0.0, float(duration or 0.0))
        elif event_type == 'anomaly':
            self.log_anomaly(symbol)
        elif event_type == 'alert':
            self.inc_alerts(1)

    # -------------
    # Flush control
    # -------------
    def maybe_flush(self, force: bool = False) -> None:
        now = time.monotonic()
        if force or (now - self._last_flush) >= self.report_interval_sec:
            self._flush_row()
            self._last_flush = now
            self._reset_interval_state()

    def shutdown(self) -> None:
        try:
            self.maybe_flush(force=True)
        except Exception as e:
            print(f"[BenchmarkLogger] shutdown flush failed: {e}")

    # ----------------
    # Internal helpers
    # ----------------
    def _init_csv(self) -> None:
        header = [
            # Existing-style columns
            "timestamp",
            "total_trades",
            "total_anomalies",
            "detection_rate_pct",
            "avg_processing_time_sec",
            "avg_detection_time_sec",
            "total_alerts",
            "memory_mb",
            "cpu_pct",
            # New columns
            "run_id",
            "env_mode",
            "replica",
            "partitions",
            "e2e_p50_ms",
            "e2e_p95_ms",
            "e2e_p99_ms",
            "e2e_max_ms",
            "lag_min",
            "lag_avg",
            "lag_max",
            "refits",
            "refit_time_ms",
            "io_write_ms_avg",
            "bytes_written",
            "top_symbols_json",
        ]
        with open(self.csv_path, "w", newline="") as f:
            csv.writer(f).writerow(header)

    def _system_metrics(self) -> (float, float):
        mem_mb, cpu_pct = 0.0, 0.0
        if _HAS_PSUTIL:
            try:
                proc = psutil.Process(os.getpid())
                mem_mb = proc.memory_info().rss / (1024.0 * 1024.0)
                cpu_pct = psutil.cpu_percent(interval=None)
            except Exception:
                pass
        return mem_mb, cpu_pct

    def _compute_e2e_stats(self) -> (float, float, float, float):
        arr = sorted(self._e2e_ms)
        return (
            _percentile(arr, 50.0),
            _percentile(arr, 95.0),
            _percentile(arr, 99.0),
            (arr[-1] if arr else 0.0),
        )

    def _compute_lag_stats(self) -> (int, float, int):
        lag_min = min(self._lag_min_samples) if self._lag_min_samples else 0
        lag_avg = (sum(self._lag_avg_samples) / len(self._lag_avg_samples)) if self._lag_avg_samples else 0.0
        lag_max = max(self._lag_max_samples) if self._lag_max_samples else 0
        return lag_min, lag_avg, lag_max

    def _top_symbols_compact_json(self, top_n: int = 5) -> str:
        items: List[Dict[str, Any]] = []
        for sym, trades in self._trades_per_symbol.items():
            anoms = self._anoms_per_symbol.get(sym, 0)
            rate = (anoms / trades * 100.0) if trades else 0.0
            e2e_list = sorted(self._e2e_ms_per_symbol.get(sym, []))
            p95 = _percentile(e2e_list, 95.0) if e2e_list else 0.0
            items.append({
                "symbol": sym,
                "trades": trades,
                "anomalies": anoms,
                "detect_rate_pct": round(rate, 3),
                "e2e_p95_ms": round(p95, 3)
            })
        items.sort(key=lambda d: (d["trades"], d["anomalies"]), reverse=True)
        return json.dumps(items[:top_n])

    def _flush_row(self) -> None:
        detection_rate_pct = (self.total_anomalies / self.total_trades * 100.0) if self.total_trades else 0.0
        avg_processing_time_sec = (self._sum_processing_time / self.total_trades) if self.total_trades else 0.0
        avg_detection_time_sec = (self._sum_detection_time / self.total_trades) if self.total_trades else 0.0

        e2e_p50_ms, e2e_p95_ms, e2e_p99_ms, e2e_max_ms = self._compute_e2e_stats()
        lag_min, lag_avg, lag_max = self._compute_lag_stats()
        io_write_ms_avg = (sum(self._io_write_ms_samples) / len(self._io_write_ms_samples)) if self._io_write_ms_samples else 0.0
        top_symbols_json = self._top_symbols_compact_json()

        mem_mb, cpu_pct = self._system_metrics()

        row = [
            _iso_utc(),
            self.total_trades,
            self.total_anomalies,
            round(detection_rate_pct, 6),
            round(avg_processing_time_sec, 6),
            round(avg_detection_time_sec, 6),
            self.total_alerts,
            round(mem_mb, 3),
            round(cpu_pct, 3),
            # New
            self.run_metadata.get("run_id", ""),
            self.run_metadata.get("env_mode", ""),
            self.run_metadata.get("replica", ""),
            self.run_metadata.get("partitions", 0),
            round(e2e_p50_ms, 3),
            round(e2e_p95_ms, 3),
            round(e2e_p99_ms, 3),
            round(e2e_max_ms, 3),
            int(lag_min),
            round(lag_avg, 3),
            int(lag_max),
            int(self._refits),
            round(self._refit_time_ms_sum, 3),
            round(io_write_ms_avg, 3),
            int(self._bytes_written_sum),
            top_symbols_json,
        ]

        with open(self.csv_path, "a", newline="") as f:
            csv.writer(f).writerow(row)

        # Also write a compact rolling summary JSON
        try:
            summary = {
                "timestamp": row[0],
                "total_trades": self.total_trades,
                "total_anomalies": self.total_anomalies,
                "detection_rate_pct": row[3],
                "avg_processing_time_sec": row[4],
                "avg_detection_time_sec": row[5],
                "total_alerts": self.total_alerts,
                "memory_mb": row[7],
                "cpu_pct": row[8],
                "run_id": self.run_metadata.get("run_id", ""),
                "env_mode": self.run_metadata.get("env_mode", ""),
                "replica": self.run_metadata.get("replica", ""),
                "partitions": self.run_metadata.get("partitions", 0),
                "e2e_p50_ms": row[13],
                "e2e_p95_ms": row[14],
                "e2e_p99_ms": row[15],
                "e2e_max_ms": row[16],
                "lag_min": row[17],
                "lag_avg": row[18],
                "lag_max": row[19],
                "refits": row[20],
                "refit_time_ms": row[21],
                "io_write_ms_avg": row[22],
                "bytes_written": row[23],
                "top_symbols_json": row[24],
            }
            with open(self.json_path, "w") as jf:
                json.dump(summary, jf, indent=2)
        except Exception as e:
            print(f"[BenchmarkLogger] Failed to write summary JSON: {e}")

    def _reset_interval_state(self) -> None:
        # Keep cumulative totals, reset interval buffers
        self._e2e_ms.clear()
        self._lag_min_samples.clear()
        self._lag_avg_samples.clear()
        self._lag_max_samples.clear()
        self._io_write_ms_samples.clear()
        self._bytes_written_sum = 0
        self._refits = 0
        self._refit_time_ms_sum = 0.0
        # Reset per-symbol E2E samples (but keep cumulative counts)
        for sym in list(self._e2e_ms_per_symbol.keys()):
            self._e2e_ms_per_symbol[sym].clear()


# Backwards-compatible alias
Benchmark = BenchmarkLogger
