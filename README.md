# Ray

This folder contains Part 2 of Course 22971: a hands-on Ray sequence covering core execution primitives, local and Docker-backed clusters, system-design patterns, Ray Data, and a final capstone project.

## Start here

- Unit 0: [Core Primitives](0_core_primitives)
- Unit 1: [Docker Cluster Setup](1_cluster_setup/0_docker_cluster_setup.md)
- Unit 2: [Distributed Systems Design Through Classical Examples](2_system_design/README.md)
- Unit 3: [Sharded Data](3_ray_data/ray_data.ipynb)
- Unit 4: [Capstone Project Design Doc](4_ray_capstone_project/design_doc.md)

## Setup

This part keeps its own Conda spec in [environment.yml](environment.yml).
Create it:

```powershell
conda env create -f environment.yml
```

Most local notebooks and `ray` CLI commands assume the `22971-ray` environment:

```powershell
conda activate 22971-ray
```

## Unit 4 Capstone Submission

The capstone code is in [4_ray_capstone_project](4_ray_capstone_project).

### Exact Local Setup

From the `Ray` folder:

```bash
conda env create -f environment.yml
conda activate 22971-ray
cd 4_ray_capstone_project
```

### Exact Local Script Commands

Prepare the real TLC Green Taxi data and replay assets:

```bash
python prepare.py --data-dir data --output-dir prepared --reference-month 2023-01 --replay-month 2023-02 --n-zones 6 --tick-minutes 15 --max-ticks 48
```

Run blocking, async, and stress replay modes locally:

```bash
python run.py --prepared-dir prepared --output-dir outputs --mode all
```

### Docker-Cluster Run

In GitHub Codespaces or another Linux environment with Docker Engine:

```bash
cd 4_ray_capstone_project
bash ./run_on_docker_engine.sh --workers 2
```

The helper starts the course Docker Ray cluster and submits this Ray Jobs command from inside `ray-head`:

```bash
ray job submit --address http://127.0.0.1:8265 --working-dir /workspace/ray_capstone_submission -- bash -lc '/opt/conda/envs/22971-ray/bin/python prepare.py --data-dir /workspace/ray_capstone/data --output-dir /workspace/ray_capstone/prepared --reference-month 2023-01 --replay-month 2023-02 --n-zones 6 --tick-minutes 15 --max-ticks 48 && RAY_ADDRESS=auto /opt/conda/envs/22971-ray/bin/python run.py --prepared-dir /workspace/ray_capstone/prepared --output-dir /workspace/ray_capstone/outputs --mode all --ray-address auto'
```

The Ray dashboard is exposed on port `8265`.

### Decision Rule

Each zone actor gives the scoring task the current demand, recent mean demand, and January baseline for the same zone/time pattern.

The task returns `NEED` when:

```text
current demand > max(baseline_count * need_multiplier, recent_mean + 1)
```

Otherwise it returns `OK`.

### Partial-Readiness Policy

Blocking mode waits for all zone tasks before closing a tick.

Async mode closes a tick when either:

- at least `completion_fraction` of zones have reported, or
- `tick_timeout_s` expires.

Late or missing zones use `fallback_policy=always_previous`. If a zone has no previous accepted decision, the first fallback is `OK`.

### Output Artifacts

Preparation writes:

```text
prepared/active_zones.json
prepared/baseline.parquet
prepared/cross_check.json
prepared/prepare_config.json
prepared/prepare_summary.md
prepared/replay_table.parquet
```

Replay writes:

```text
outputs/notebook_blocking/
outputs/notebook_async/
outputs/notebook_stress/
outputs/demo_summary.csv
outputs/demo_summary.json
outputs/demo_talking_points.md
```

Each run folder contains:

```text
run_config.json
metrics.csv
latency_log.json
tick_summary.json
decisions.parquet
actor_counters.json
```

In the Docker run, artifacts are written under:

```text
1_cluster_setup/head_workspace/ray_capstone
```
