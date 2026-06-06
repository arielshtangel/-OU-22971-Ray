from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

import pandas as pd

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
PICKUP_COL = "lpep_pickup_datetime"
ZONE_COL = "PULocationID"


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


def file_name(month: str) -> str:
    return f"green_tripdata_{month}.parquet"


def download_month(month: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / file_name(month)
    if path.exists():
        print(f"Using existing {path}")
        return path

    url = f"{BASE_URL}/{path.name}"
    print(f"Downloading {url}")
    urlretrieve(url, path)
    return path


def download_data(output_dir: Path, months: list[str]) -> list[Path]:
    paths = [download_month(month, output_dir) for month in months]
    manifest = {
        "data_dir": str(output_dir),
        "files": [
            {
                "month": month,
                "path": str(path),
                "size_mb": round(path.stat().st_size / (1024 * 1024), 2),
            }
            for month, path in zip(months, paths)
        ],
    }
    write_json(output_dir / "download_manifest.json", manifest)
    return paths


def require_columns(frame: pd.DataFrame, path: Path) -> None:
    missing = [col for col in (PICKUP_COL, ZONE_COL) if col not in frame.columns]
    if missing:
        raise ValueError(f"{path} is missing required columns: {missing}")


def dominant_month(frame: pd.DataFrame, label: str) -> tuple[pd.Period, dict[str, int]]:
    dates = pd.to_datetime(frame[PICKUP_COL], errors="coerce")
    month_counts = dates.dt.to_period("M").value_counts().sort_index()
    if month_counts.empty:
        raise ValueError(f"{label} file has no valid pickup timestamps")
    dominant = month_counts.idxmax()
    return dominant, {str(key): int(value) for key, value in month_counts.items()}


def validate_adjacent_months(
    reference: pd.DataFrame, replay: pd.DataFrame
) -> tuple[pd.Period, pd.Period, dict[str, int], dict[str, int]]:
    ref_month, ref_counts = dominant_month(reference, "reference")
    replay_month, replay_counts = dominant_month(replay, "replay")
    if replay_month != ref_month + 1:
        raise ValueError(
            f"replay month must immediately follow reference month; got {ref_month} -> {replay_month}"
        )
    return ref_month, replay_month, ref_counts, replay_counts


def add_tick_columns(frame: pd.DataFrame, tick_minutes: int) -> pd.DataFrame:
    out = frame[[PICKUP_COL, ZONE_COL]].copy()
    out[PICKUP_COL] = pd.to_datetime(out[PICKUP_COL], errors="coerce")
    out[ZONE_COL] = pd.to_numeric(out[ZONE_COL], errors="coerce")
    out = out.dropna(subset=[PICKUP_COL, ZONE_COL]).copy()
    out["zone_id"] = out[ZONE_COL].astype(int)
    out["tick_start"] = out[PICKUP_COL].dt.floor(f"{tick_minutes}min")
    out["hour_of_day"] = out["tick_start"].dt.hour.astype(int)
    out["day_of_week"] = out["tick_start"].dt.dayofweek.astype(int)
    return out


def filter_to_month(frame: pd.DataFrame, month: pd.Period) -> pd.DataFrame:
    return frame[frame["tick_start"].dt.to_period("M") == month].copy()


def select_active_zones(reference: pd.DataFrame, n_zones: int) -> list[int]:
    zone_counts = (
        reference.groupby("zone_id")
        .size()
        .rename("pickup_count")
        .reset_index()
        .sort_values(["pickup_count", "zone_id"], ascending=[False, True])
    )
    if zone_counts.empty:
        raise ValueError("reference data has no pickup zones")
    return [int(zone_id) for zone_id in zone_counts.head(n_zones)["zone_id"]]


def aggregate_counts(frame: pd.DataFrame) -> pd.DataFrame:
    return (
        frame.groupby(["zone_id", "tick_start", "hour_of_day", "day_of_week"])
        .size()
        .rename("demand_count")
        .reset_index()
    )


def build_baseline(reference_counts: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, float]:
    baseline = (
        reference_counts.groupby(["zone_id", "hour_of_day", "day_of_week"])["demand_count"]
        .mean()
        .rename("baseline_count")
        .reset_index()
    )
    zone_defaults = reference_counts.groupby("zone_id")["demand_count"].mean()
    global_default = float(reference_counts["demand_count"].mean())
    return baseline, zone_defaults, global_default


def build_replay_table(
    replay: pd.DataFrame,
    active_zones: list[int],
    baseline: pd.DataFrame,
    zone_defaults: pd.Series,
    global_default: float,
    tick_minutes: int,
) -> pd.DataFrame:
    replay_counts = aggregate_counts(replay[replay["zone_id"].isin(active_zones)])
    if replay_counts.empty:
        raise ValueError("replay data has no pickups for the selected active zones")

    tick_starts = pd.date_range(
        start=replay_counts["tick_start"].min(),
        end=replay_counts["tick_start"].max(),
        freq=f"{tick_minutes}min",
    )
    grid = pd.MultiIndex.from_product(
        [active_zones, tick_starts], names=["zone_id", "tick_start"]
    ).to_frame(index=False)
    grid["hour_of_day"] = grid["tick_start"].dt.hour.astype(int)
    grid["day_of_week"] = grid["tick_start"].dt.dayofweek.astype(int)

    replay_table = grid.merge(
        replay_counts[["zone_id", "tick_start", "demand_count"]],
        on=["zone_id", "tick_start"],
        how="left",
    )
    replay_table["demand_count"] = replay_table["demand_count"].fillna(0).astype(int)
    replay_table = replay_table.merge(
        baseline, on=["zone_id", "hour_of_day", "day_of_week"], how="left"
    )
    replay_table["baseline_count"] = replay_table["baseline_count"].fillna(
        replay_table["zone_id"].map(zone_defaults)
    )
    replay_table["baseline_count"] = replay_table["baseline_count"].fillna(global_default)
    tick_ids = {tick: idx for idx, tick in enumerate(sorted(tick_starts))}
    replay_table["tick_id"] = replay_table["tick_start"].map(tick_ids).astype(int)
    return replay_table.sort_values(["tick_id", "zone_id"]).reset_index(drop=True)


def cross_check_replay(
    raw_replay: pd.DataFrame,
    replay_table: pd.DataFrame,
    active_zones: list[int],
    sample_ticks: int,
    tick_minutes: int,
) -> dict[str, object]:
    tick_values = sorted(replay_table["tick_start"].unique())
    selected_ticks = tick_values[: min(sample_ticks, len(tick_values))]
    sample_start = pd.Timestamp(selected_ticks[0])
    sample_end = pd.Timestamp(selected_ticks[-1]) + pd.Timedelta(minutes=tick_minutes)

    direct = raw_replay[
        raw_replay["zone_id"].isin(active_zones)
        & (raw_replay["tick_start"] >= sample_start)
        & (raw_replay["tick_start"] < sample_end)
    ]
    direct_total = int(len(direct))
    prepared_total = int(
        replay_table[replay_table["tick_start"].isin(selected_ticks)]["demand_count"].sum()
    )
    return {
        "sample_start": sample_start.isoformat(),
        "sample_end_exclusive": sample_end.isoformat(),
        "sample_ticks": len(selected_ticks),
        "direct_total": direct_total,
        "prepared_total": prepared_total,
        "passed": direct_total == prepared_total,
    }


def write_prepare_summary(output_dir: Path, active_zones: list[int], replay_table: pd.DataFrame, check: dict[str, object], tick_minutes: int) -> None:
    lines = [
        "# Preparation Evidence",
        "",
        f"- Active zones: {len(active_zones)}",
        f"- Replay ticks: {replay_table['tick_id'].nunique()}",
        f"- Tick length minutes: {tick_minutes}",
        f"- Replay table rows: {len(replay_table)}",
        f"- Cross-check sample ticks: {check['sample_ticks']}",
        f"- Raw replay rows in sample: {check['direct_total']}",
        f"- Prepared demand total in sample: {check['prepared_total']}",
        f"- Cross-check passed: {check['passed']}",
    ]
    (output_dir / "prepare_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def prepare_assets(
    reference_parquet: Path,
    replay_parquet: Path,
    output_dir: Path,
    n_zones: int,
    tick_minutes: int,
    seed: int,
    max_ticks: int | None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    reference_raw = pd.read_parquet(reference_parquet)
    replay_raw = pd.read_parquet(replay_parquet)
    require_columns(reference_raw, reference_parquet)
    require_columns(replay_raw, replay_parquet)
    ref_month, replay_month, ref_month_counts, replay_month_counts = validate_adjacent_months(
        reference_raw, replay_raw
    )

    reference = filter_to_month(add_tick_columns(reference_raw, tick_minutes), ref_month)
    replay = filter_to_month(add_tick_columns(replay_raw, tick_minutes), replay_month)
    active_zones = select_active_zones(reference, n_zones=n_zones)

    reference_counts = aggregate_counts(reference[reference["zone_id"].isin(active_zones)])
    baseline, zone_defaults, global_default = build_baseline(reference_counts)
    replay_table = build_replay_table(
        replay=replay,
        active_zones=active_zones,
        baseline=baseline,
        zone_defaults=zone_defaults,
        global_default=global_default,
        tick_minutes=tick_minutes,
    )
    if max_ticks is not None:
        keep_tick_ids = sorted(replay_table["tick_id"].unique())[:max_ticks]
        replay_table = replay_table[replay_table["tick_id"].isin(keep_tick_ids)].copy()

    check = cross_check_replay(
        raw_replay=replay,
        replay_table=replay_table,
        active_zones=active_zones,
        sample_ticks=8,
        tick_minutes=tick_minutes,
    )
    if not check["passed"]:
        raise ValueError(f"prepared replay cross-check failed: {check}")

    baseline.to_parquet(output_dir / "baseline.parquet", index=False)
    replay_table.to_parquet(output_dir / "replay_table.parquet", index=False)
    write_json(output_dir / "active_zones.json", active_zones)
    write_json(output_dir / "cross_check.json", check)
    write_json(
        output_dir / "prepare_config.json",
        {
            "reference_parquet": str(reference_parquet),
            "replay_parquet": str(replay_parquet),
            "reference_month": str(ref_month),
            "replay_month": str(replay_month),
            "reference_month_counts": ref_month_counts,
            "replay_month_counts": replay_month_counts,
            "n_zones": len(active_zones),
            "tick_minutes": tick_minutes,
            "seed": seed,
            "max_ticks": max_ticks,
            "first_use_fallback_decision": "OK",
        },
    )
    write_prepare_summary(output_dir, active_zones, replay_table, check, tick_minutes)

    print("Preparation evidence")
    print("--------------------")
    print(f"Prepared zones: {len(active_zones)}")
    print(f"Prepared ticks: {replay_table['tick_id'].nunique()}")
    print(f"Cross-check passed: {check}")
    print(f"Artifacts written to: {output_dir}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download and prepare TLC Green Taxi replay assets.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("prepared"))
    parser.add_argument("--reference-month", default="2023-01")
    parser.add_argument("--replay-month", default="2023-02")
    parser.add_argument("--n-zones", type=int, default=6)
    parser.add_argument("--tick-minutes", type=int, default=15)
    parser.add_argument("--seed", type=int, default=22971)
    parser.add_argument("--max-ticks", type=int, default=48)
    parser.add_argument("--skip-download", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    months = [args.reference_month, args.replay_month]
    if not args.skip_download:
        download_data(args.data_dir, months)

    reference_path = args.data_dir / file_name(args.reference_month)
    replay_path = args.data_dir / file_name(args.replay_month)
    prepare_assets(
        reference_parquet=reference_path,
        replay_parquet=replay_path,
        output_dir=args.output_dir,
        n_zones=args.n_zones,
        tick_minutes=args.tick_minutes,
        seed=args.seed,
        max_ticks=args.max_ticks,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
