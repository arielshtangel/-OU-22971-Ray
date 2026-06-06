from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from argparse import Namespace
from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd
import ray

DECISION_NEED = "NEED"
DECISION_OK = "OK"


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if hasattr(value, "item"):
        return value.item()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True), encoding="utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def stable_fraction(seed: int, zone_id: int, tick_id: int, salt: str) -> float:
    raw = f"{seed}:{zone_id}:{tick_id}:{salt}".encode("utf-8")
    value = int(hashlib.sha256(raw).hexdigest()[:16], 16)
    return value / float(0xFFFFFFFFFFFFFFFF)


@ray.remote
class ZoneActor:
    def __init__(self, zone_id: int, replay_rows: list[dict[str, Any]]) -> None:
        self.zone_id = int(zone_id)
        self.rows_by_tick = {int(row["tick_id"]): row for row in replay_rows}
        self.active_tick_id: int | None = None
        self.active_closed = False
        self.reported_payload: dict[str, Any] | None = None
        self.history: dict[int, dict[str, Any]] = {}
        self.recent_demands: list[int] = []
        self.last_decision: str | None = None
        self.counters = {
            "duplicate_writes": 0,
            "duplicate_reports": 0,
            "late_reports": 0,
            "fallbacks": 0,
        }

    def start_tick(self, tick_id: int) -> dict[str, Any]:
        tick_id = int(tick_id)
        self.active_tick_id = tick_id
        self.active_closed = tick_id in self.history
        self.reported_payload = None
        return {
            "zone_id": self.zone_id,
            "tick_id": tick_id,
            "already_finalized": self.active_closed,
        }

    def snapshot(self, tick_id: int) -> dict[str, Any]:
        tick_id = int(tick_id)
        row = self._row_for_tick(tick_id)
        recent = self.recent_demands[-4:]
        baseline = float(row["baseline_count"])
        recent_mean = float(sum(recent) / len(recent)) if recent else baseline
        return {
            "zone_id": self.zone_id,
            "tick_id": tick_id,
            "tick_start": row["tick_start"],
            "demand_count": int(row["demand_count"]),
            "baseline_count": baseline,
            "recent_mean": recent_mean,
            "previous_decision": self.last_decision or DECISION_OK,
        }

    def report_decision(self, payload: dict[str, Any]) -> dict[str, Any]:
        tick_id = int(payload["tick_id"])
        if tick_id in self.history:
            self.counters["late_reports"] += 1
            return {"accepted": False, "reason": "late_closed", "tick_id": tick_id}
        if self.active_tick_id != tick_id or self.active_closed:
            self.counters["late_reports"] += 1
            return {"accepted": False, "reason": "inactive_or_closed", "tick_id": tick_id}
        if self.reported_payload is not None:
            self.counters["duplicate_reports"] += 1
            return {"accepted": False, "reason": "duplicate_report", "tick_id": tick_id}

        self.reported_payload = deepcopy(payload)
        return {"accepted": True, "reason": "reported", "tick_id": tick_id}

    def ready_status(self, tick_id: int) -> dict[str, Any]:
        tick_id = int(tick_id)
        return {
            "zone_id": self.zone_id,
            "tick_id": tick_id,
            "has_report": self.reported_payload is not None
            and int(self.reported_payload["tick_id"]) == tick_id,
            "is_closed": tick_id in self.history,
        }

    def write_decision(self, payload: dict[str, Any], used_fallback: bool = False) -> dict[str, Any]:
        tick_id = int(payload["tick_id"])
        if tick_id in self.history:
            self.counters["duplicate_writes"] += 1
            return deepcopy(self.history[tick_id])
        if self.active_tick_id != tick_id:
            self.counters["late_reports"] += 1
            return {
                "zone_id": self.zone_id,
                "tick_id": tick_id,
                "accepted": False,
                "reason": "write_for_inactive_tick",
            }
        return self._accept(payload=payload, used_fallback=used_fallback, source="controller")

    def finalize_tick(self, tick_id: int, fallback_policy: str) -> dict[str, Any]:
        tick_id = int(tick_id)
        if tick_id in self.history:
            self.counters["duplicate_writes"] += 1
            return deepcopy(self.history[tick_id])
        if self.active_tick_id != tick_id:
            raise ValueError(f"zone {self.zone_id} cannot finalize inactive tick {tick_id}")

        if self.reported_payload is not None:
            return self._accept(
                payload=self.reported_payload,
                used_fallback=False,
                source="reported",
            )

        if fallback_policy != "always_previous":
            raise ValueError(f"unsupported fallback_policy={fallback_policy}")

        row = self._row_for_tick(tick_id)
        fallback_decision = self.last_decision or DECISION_OK
        payload = {
            "zone_id": self.zone_id,
            "tick_id": tick_id,
            "tick_start": row["tick_start"],
            "demand_count": int(row["demand_count"]),
            "baseline_count": float(row["baseline_count"]),
            "recent_mean": float(row["baseline_count"]),
            "decision": fallback_decision,
            "threshold": None,
            "task_latency_s": None,
            "slow_task": False,
        }
        self.counters["fallbacks"] += 1
        return self._accept(payload=payload, used_fallback=True, source="fallback")

    def get_history(self) -> list[dict[str, Any]]:
        return [deepcopy(self.history[key]) for key in sorted(self.history)]

    def get_counters(self) -> dict[str, Any]:
        out = {"zone_id": self.zone_id}
        out.update(self.counters)
        return out

    def _row_for_tick(self, tick_id: int) -> dict[str, Any]:
        row = self.rows_by_tick.get(int(tick_id))
        if row is None:
            raise ValueError(f"zone {self.zone_id} has no prepared row for tick {tick_id}")
        return row

    def _accept(
        self,
        payload: dict[str, Any],
        used_fallback: bool,
        source: str,
    ) -> dict[str, Any]:
        tick_id = int(payload["tick_id"])
        row = self._row_for_tick(tick_id)
        record = {
            "zone_id": self.zone_id,
            "tick_id": tick_id,
            "tick_start": row["tick_start"],
            "demand_count": int(row["demand_count"]),
            "baseline_count": float(row["baseline_count"]),
            "recent_mean": float(payload.get("recent_mean", 0.0)),
            "decision": str(payload["decision"]),
            "threshold": payload.get("threshold"),
            "task_latency_s": payload.get("task_latency_s"),
            "slow_task": bool(payload.get("slow_task", False)),
            "used_fallback": bool(used_fallback),
            "source": source,
        }
        self.history[tick_id] = record
        self.recent_demands.append(int(row["demand_count"]))
        self.last_decision = record["decision"]
        self.active_closed = True
        return deepcopy(record)


@ray.remote(max_retries=1)
def score_zone(
    snapshot: dict[str, Any],
    config: dict[str, Any],
    zone_actor: Any | None = None,
) -> dict[str, Any]:
    start = time.perf_counter()
    zone_id = int(snapshot["zone_id"])
    tick_id = int(snapshot["tick_id"])
    slow_task = (
        stable_fraction(int(config["seed"]), zone_id, tick_id, "slow")
        < float(config["slow_zone_fraction"])
    )
    if slow_task:
        time.sleep(float(config["slow_zone_sleep_s"]))

    baseline = float(snapshot["baseline_count"])
    recent_mean = float(snapshot["recent_mean"])
    demand = int(snapshot["demand_count"])
    threshold = max(baseline * float(config["need_multiplier"]), recent_mean + 1.0)
    decision = DECISION_NEED if demand > threshold else DECISION_OK
    payload = {
        "zone_id": zone_id,
        "tick_id": tick_id,
        "tick_start": snapshot["tick_start"],
        "demand_count": demand,
        "baseline_count": baseline,
        "recent_mean": recent_mean,
        "decision": decision,
        "threshold": threshold,
        "task_latency_s": time.perf_counter() - start,
        "slow_task": slow_task,
    }

    if zone_actor is not None:
        ray.get(zone_actor.report_decision.remote(payload))
        duplicate = (
            stable_fraction(int(config["seed"]), zone_id, tick_id, "duplicate")
            < float(config["duplicate_report_fraction"])
        )
        if duplicate:
            ray.get(zone_actor.report_decision.remote(payload))

    return payload


def load_prepared(prepared_dir: Path, n_zones: int | None) -> tuple[pd.DataFrame, list[int], dict[str, Any]]:
    active_zones = [int(zone) for zone in load_json(prepared_dir / "active_zones.json")]
    if n_zones is not None:
        active_zones = active_zones[:n_zones]
    replay = pd.read_parquet(prepared_dir / "replay_table.parquet")
    replay = replay[replay["zone_id"].isin(active_zones)].copy()
    replay["tick_start"] = pd.to_datetime(replay["tick_start"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    replay["zone_id"] = replay["zone_id"].astype(int)
    replay["tick_id"] = replay["tick_id"].astype(int)
    replay["demand_count"] = replay["demand_count"].astype(int)
    replay["baseline_count"] = replay["baseline_count"].astype(float)
    prepare_config = load_json(prepared_dir / "prepare_config.json")
    return replay.sort_values(["tick_id", "zone_id"]), active_zones, prepare_config


def build_actors(replay: pd.DataFrame, active_zones: list[int]) -> dict[int, Any]:
    actors: dict[int, Any] = {}
    for zone_id in active_zones:
        rows = replay[replay["zone_id"] == zone_id].sort_values("tick_id").to_dict("records")
        actors[zone_id] = ZoneActor.remote(zone_id, rows)
    return actors


def scoring_config(args: Namespace) -> dict[str, Any]:
    return {
        "seed": int(args.seed),
        "slow_zone_fraction": float(args.slow_zone_fraction),
        "slow_zone_sleep_s": float(args.slow_zone_sleep_s),
        "need_multiplier": float(args.need_multiplier),
        "duplicate_report_fraction": float(args.duplicate_report_fraction),
    }


def run_config(args: Namespace, prepare_config: dict[str, Any], active_zones: list[int]) -> dict[str, Any]:
    return {
        "mode": args.mode,
        "n_zones": len(active_zones),
        "active_zones": active_zones,
        "tick_minutes": prepare_config.get("tick_minutes"),
        "max_inflight_zones": args.max_inflight_zones,
        "tick_timeout_s": args.tick_timeout_s,
        "completion_fraction": args.completion_fraction,
        "slow_zone_fraction": args.slow_zone_fraction,
        "slow_zone_sleep_s": args.slow_zone_sleep_s,
        "fallback_policy": args.fallback_policy,
        "first_use_fallback_decision": "OK",
        "seed": args.seed,
        "need_multiplier": args.need_multiplier,
        "duplicate_report_fraction": args.duplicate_report_fraction,
    }


def summarize_tick(
    mode: str,
    tick_id: int,
    tick_latency_s: float,
    records: list[dict[str, Any]],
    completed_before_finalize: int,
    policy_reason: str,
) -> dict[str, Any]:
    latencies = [
        float(record["task_latency_s"])
        for record in records
        if record.get("task_latency_s") is not None
    ]
    mean_latency = sum(latencies) / len(latencies) if latencies else 0.0
    max_latency = max(latencies) if latencies else 0.0
    return {
        "mode": mode,
        "tick_id": int(tick_id),
        "tick_latency_s": tick_latency_s,
        "mean_zone_latency_s": mean_latency,
        "max_zone_latency_s": max_latency,
        "max_mean_latency_ratio": max_latency / mean_latency if mean_latency else 0.0,
        "completed_before_finalize": int(completed_before_finalize),
        "fallback_count": int(sum(1 for record in records if record.get("used_fallback"))),
        "policy_reason": policy_reason,
    }


def collect_actor_state(actors: dict[int, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    histories = ray.get([actor.get_history.remote() for actor in actors.values()])
    counters = ray.get([actor.get_counters.remote() for actor in actors.values()])
    decisions = [record for history in histories for record in history]
    decisions.sort(key=lambda item: (int(item["tick_id"]), int(item["zone_id"])))
    counters.sort(key=lambda item: int(item["zone_id"]))
    return decisions, counters


def write_run_artifacts(
    output_dir: Path,
    config: dict[str, Any],
    tick_summaries: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    counters: list[dict[str, Any]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "run_config.json", config)
    pd.DataFrame(tick_summaries).to_csv(output_dir / "metrics.csv", index=False)
    write_json(output_dir / "tick_summary.json", tick_summaries)
    latency_log = [
        {
            "zone_id": record["zone_id"],
            "tick_id": record["tick_id"],
            "task_latency_s": record["task_latency_s"],
            "slow_task": record["slow_task"],
            "used_fallback": record["used_fallback"],
        }
        for record in decisions
    ]
    write_json(output_dir / "latency_log.json", latency_log)
    pd.DataFrame(decisions).to_parquet(output_dir / "decisions.parquet", index=False)
    write_json(output_dir / "actor_counters.json", counters)


def run_blocking(prepared_dir: Path, output_dir: Path, args: Namespace) -> list[dict[str, Any]]:
    replay, active_zones, prepare_config = load_prepared(prepared_dir, args.n_zones)
    actors = build_actors(replay, active_zones)
    tick_summaries: list[dict[str, Any]] = []
    config = scoring_config(args)

    for tick_id in sorted(replay["tick_id"].unique()):
        tick_start_time = time.perf_counter()
        ray.get([actor.start_tick.remote(int(tick_id)) for actor in actors.values()])
        snapshots = ray.get([actor.snapshot.remote(int(tick_id)) for actor in actors.values()])
        result_refs = [score_zone.remote(snapshot, config) for snapshot in snapshots]
        payloads = ray.get(result_refs)
        records = ray.get(
            [
                actors[int(payload["zone_id"])].write_decision.remote(payload, False)
                for payload in payloads
            ]
        )
        tick_latency_s = time.perf_counter() - tick_start_time
        tick_summaries.append(
            summarize_tick(
                mode="blocking",
                tick_id=int(tick_id),
                tick_latency_s=tick_latency_s,
                records=records,
                completed_before_finalize=len(active_zones),
                policy_reason="all_results_returned",
            )
        )

    decisions, counters = collect_actor_state(actors)
    write_run_artifacts(
        output_dir=output_dir,
        config=run_config(args, prepare_config, active_zones),
        tick_summaries=tick_summaries,
        decisions=decisions,
        counters=counters,
    )
    print(f"Blocking run wrote artifacts to {output_dir}")
    return tick_summaries


def launch_until_limit(
    pending_snapshots: list[dict[str, Any]],
    in_flight: list[Any],
    actors: dict[int, Any],
    config: dict[str, Any],
    limit: int,
) -> None:
    while pending_snapshots and len(in_flight) < limit:
        snapshot = pending_snapshots.pop(0)
        actor = actors[int(snapshot["zone_id"])]
        in_flight.append(score_zone.remote(snapshot, config, actor))


def run_async(prepared_dir: Path, output_dir: Path, args: Namespace) -> list[dict[str, Any]]:
    replay, active_zones, prepare_config = load_prepared(prepared_dir, args.n_zones)
    actors = build_actors(replay, active_zones)
    tick_summaries: list[dict[str, Any]] = []
    config = scoring_config(args)
    background_refs: list[Any] = []

    for tick_id in sorted(replay["tick_id"].unique()):
        tick_start_time = time.perf_counter()
        ray.get([actor.start_tick.remote(int(tick_id)) for actor in actors.values()])
        snapshots = ray.get([actor.snapshot.remote(int(tick_id)) for actor in actors.values()])
        pending_snapshots = sorted(snapshots, key=lambda item: int(item["zone_id"]))
        in_flight: list[Any] = []
        launch_until_limit(
            pending_snapshots=pending_snapshots,
            in_flight=in_flight,
            actors=actors,
            config=config,
            limit=max(1, int(args.max_inflight_zones)),
        )

        policy_reason = "timeout"
        completed_before_finalize = 0
        while True:
            if in_flight:
                done_refs, remaining_refs = ray.wait(
                    in_flight,
                    num_returns=len(in_flight),
                    timeout=0,
                )
                if done_refs:
                    ray.get(done_refs)
                in_flight = list(remaining_refs)
                launch_until_limit(
                    pending_snapshots=pending_snapshots,
                    in_flight=in_flight,
                    actors=actors,
                    config=config,
                    limit=max(1, int(args.max_inflight_zones)),
                )

            statuses = ray.get(
                [actor.ready_status.remote(int(tick_id)) for actor in actors.values()]
            )
            completed_before_finalize = sum(1 for item in statuses if item["has_report"])
            elapsed = time.perf_counter() - tick_start_time
            if completed_before_finalize / len(active_zones) >= float(args.completion_fraction):
                policy_reason = "completion_fraction"
                break
            if elapsed >= float(args.tick_timeout_s):
                policy_reason = "timeout"
                break
            if not pending_snapshots and not in_flight and completed_before_finalize == len(active_zones):
                policy_reason = "all_results_returned"
                break
            time.sleep(float(args.poll_interval_s))

        records = ray.get(
            [
                actor.finalize_tick.remote(int(tick_id), args.fallback_policy)
                for actor in actors.values()
            ]
        )
        tick_latency_s = time.perf_counter() - tick_start_time
        tick_summaries.append(
            summarize_tick(
                mode="async",
                tick_id=int(tick_id),
                tick_latency_s=tick_latency_s,
                records=records,
                completed_before_finalize=completed_before_finalize,
                policy_reason=policy_reason,
            )
        )
        background_refs.extend(in_flight)

    if background_refs:
        done_refs, _ = ray.wait(
            background_refs,
            num_returns=len(background_refs),
            timeout=float(args.late_drain_s),
        )
        if done_refs:
            ray.get(done_refs)

    decisions, counters = collect_actor_state(actors)
    write_run_artifacts(
        output_dir=output_dir,
        config=run_config(args, prepare_config, active_zones),
        tick_summaries=tick_summaries,
        decisions=decisions,
        counters=counters,
    )
    print(f"Async run wrote artifacts to {output_dir}")
    return tick_summaries


def run_stress(prepared_dir: Path, output_dir: Path, args: Namespace) -> dict[str, Any]:
    stress_args = Namespace(**vars(args))
    stress_args.slow_zone_fraction = max(float(args.slow_zone_fraction), 0.75)
    stress_args.slow_zone_sleep_s = max(float(args.slow_zone_sleep_s), 0.6)
    stress_args.completion_fraction = min(float(args.completion_fraction), 0.5)

    blocking_args = Namespace(**vars(stress_args))
    blocking_args.mode = "stress_blocking"
    blocking_metrics = run_blocking(
        prepared_dir=prepared_dir,
        output_dir=output_dir / "blocking",
        args=blocking_args,
    )

    async_args = Namespace(**vars(stress_args))
    async_args.mode = "stress_async"
    async_metrics = run_async(
        prepared_dir=prepared_dir,
        output_dir=output_dir / "async",
        args=async_args,
    )

    comparison = {
        "blocking_total_tick_latency_s": sum(item["tick_latency_s"] for item in blocking_metrics),
        "async_total_tick_latency_s": sum(item["tick_latency_s"] for item in async_metrics),
        "blocking_total_fallbacks": sum(item["fallback_count"] for item in blocking_metrics),
        "async_total_fallbacks": sum(item["fallback_count"] for item in async_metrics),
        "slow_zone_fraction": stress_args.slow_zone_fraction,
        "slow_zone_sleep_s": stress_args.slow_zone_sleep_s,
        "completion_fraction": stress_args.completion_fraction,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "stress_comparison.json", comparison)
    print(f"Stress comparison wrote artifacts to {output_dir}")
    return comparison


def resolve_ray_address(address: str | None = None) -> str | None:
    if address:
        return address
    return os.getenv("RAY_ADDRESS") or None


def ensure_ray(address: str | None = None) -> str:
    if ray.is_initialized():
        return "already_initialized"
    resolved = resolve_ray_address(address)
    if resolved:
        ray.init(address=resolved, ignore_reinit_error=True)
        return resolved
    ray.init(ignore_reinit_error=True, include_dashboard=False)
    return "local"


def write_demo_summary(output_root: Path) -> None:
    demo_paths = {
        "blocking": output_root / "notebook_blocking",
        "async": output_root / "notebook_async",
        "stress_blocking": output_root / "notebook_stress" / "blocking",
        "stress_async": output_root / "notebook_stress" / "async",
    }
    summary_rows = []
    for name, out_dir in demo_paths.items():
        if not (out_dir / "metrics.csv").exists():
            continue
        metrics = pd.read_csv(out_dir / "metrics.csv")
        decisions = pd.read_parquet(out_dir / "decisions.parquet")
        counters = load_json(out_dir / "actor_counters.json")
        config = load_json(out_dir / "run_config.json")
        summary_rows.append(
            {
                "run": name,
                "mode": config["mode"],
                "ticks": int(len(metrics)),
                "zones": int(config["n_zones"]),
                "actor_count": int(config["n_zones"]),
                "accepted_decisions": int(len(decisions)),
                "score_task_results_used": int(decisions["task_latency_s"].notna().sum()),
                "slow_task_results": int(decisions["slow_task"].fillna(False).sum()),
                "fallback_decisions": int(metrics["fallback_count"].sum()),
                "need_decisions": int((decisions["decision"] == DECISION_NEED).sum()),
                "ok_decisions": int((decisions["decision"] == DECISION_OK).sum()),
                "total_tick_latency_s": round(float(metrics["tick_latency_s"].sum()), 3),
                "mean_tick_latency_s": round(float(metrics["tick_latency_s"].mean()), 3),
                "max_tick_latency_s": round(float(metrics["tick_latency_s"].max()), 3),
                "mean_completed_before_finalize": round(
                    float(metrics["completed_before_finalize"].mean()), 2
                ),
                "late_reports": int(sum(item.get("late_reports", 0) for item in counters)),
                "duplicate_reports": int(sum(item.get("duplicate_reports", 0) for item in counters)),
                "duplicate_writes": int(sum(item.get("duplicate_writes", 0) for item in counters)),
                "policy_reasons": metrics["policy_reason"].value_counts().to_dict(),
                "decision_sources": decisions["source"].value_counts().to_dict(),
                "artifact_dir": str(out_dir),
            }
        )

    if not summary_rows:
        return

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_root / "demo_summary.csv", index=False)
    write_json(output_root / "demo_summary.json", summary_rows)

    stress_path = output_root / "notebook_stress" / "stress_comparison.json"
    stress = load_json(stress_path) if stress_path.exists() else {}
    blocking_stress = float(stress.get("blocking_total_tick_latency_s", 0.0))
    async_stress = float(stress.get("async_total_tick_latency_s", 0.0))
    speedup = blocking_stress / async_stress if async_stress else 0.0
    lines = [
        "# Demo Talking Points",
        "",
        "- `ZoneActor` is the Ray actor. One actor owns the state for one taxi zone.",
        "- `score_zone` is the Ray task. It scores a zone/tick snapshot and returns `NEED` or `OK`.",
        f"- Stress timing: blocking={blocking_stress:.3f}s, async={async_stress:.3f}s, speedup={speedup:.2f}x.",
        f"- Stress fallbacks: blocking={stress.get('blocking_total_fallbacks', 0)}, async={stress.get('async_total_fallbacks', 0)}.",
        "- Fallbacks happen when async closes a tick before every slow zone reports.",
    ]
    (output_root / "demo_talking_points.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("Ray execution evidence")
    print("----------------------")
    print("Actor class: ZoneActor")
    print("Task function: score_zone")
    print(f"Summary saved to: {output_root / 'demo_summary.csv'}")
    print(f"Talking points saved to: {output_root / 'demo_talking_points.md'}")
    if stress:
        print(
            f"Stress timing: blocking={blocking_stress:.3f}s, "
            f"async={async_stress:.3f}s, speedup={speedup:.2f}x"
        )


def run_selected(args: Namespace) -> None:
    ray_connection = ensure_ray(args.ray_address)
    print({"ray_connection": ray_connection, "cluster_resources": ray.cluster_resources()})

    output_root = args.output_dir
    if args.mode in ("all", "blocking"):
        blocking_args = Namespace(**vars(args))
        blocking_args.mode = "blocking"
        run_blocking(args.prepared_dir, output_root / "notebook_blocking", blocking_args)

    if args.mode in ("all", "async"):
        async_args = Namespace(**vars(args))
        async_args.mode = "async"
        run_async(args.prepared_dir, output_root / "notebook_async", async_args)

    if args.mode in ("all", "stress"):
        stress_args = Namespace(**vars(args))
        stress_args.mode = "stress"
        run_stress(args.prepared_dir, output_root / "notebook_stress", stress_args)

    write_demo_summary(output_root)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run blocking, async, and stress Ray replay modes.")
    parser.add_argument("--prepared-dir", type=Path, default=Path("prepared"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    parser.add_argument("--mode", choices=("all", "blocking", "async", "stress"), default="all")
    parser.add_argument("--n-zones", type=int, default=None)
    parser.add_argument("--max-inflight-zones", type=int, default=3)
    parser.add_argument("--tick-timeout-s", type=float, default=0.25)
    parser.add_argument("--completion-fraction", type=float, default=0.67)
    parser.add_argument("--slow-zone-fraction", type=float, default=0.33)
    parser.add_argument("--slow-zone-sleep-s", type=float, default=0.30)
    parser.add_argument("--fallback-policy", default="always_previous")
    parser.add_argument("--seed", type=int, default=22971)
    parser.add_argument("--need-multiplier", type=float, default=1.40)
    parser.add_argument("--duplicate-report-fraction", type=float, default=0.15)
    parser.add_argument("--poll-interval-s", type=float, default=0.02)
    parser.add_argument("--late-drain-s", type=float, default=2.0)
    parser.add_argument("--ray-address", default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        run_selected(args)
    finally:
        ray.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
