# Ray Unit 4 Capstone Notebook Solution

This submission is a notebook-first implementation of the TLC-backed per-zone recommendation replay under skew. It can run locally from the notebooks, and it also includes a GitHub Codespaces flow for the required Docker-based Ray cluster demo.

Run the notebooks in this order:

1. `01_download_real_data.ipynb`
2. `02_prepare_assets.ipynb`
3. `03_run_replay.ipynb`

For the Docker-based demo from GitHub Codespaces, run:

4. `04_codespaces_docker_launcher.ipynb`

The optional stretch goals are not included.

## Setup

Use the Ray course environment.

```powershell
conda env create -f ..\..\environment.yml
conda activate 22971-ray
cd Ray\4_ray_capstone_project\solution
```

## Local Notebook Run

Open the notebooks from this directory and run all cells in order:

```text
01_download_real_data.ipynb
02_prepare_assets.ipynb
03_run_replay.ipynb
```

The replay notebook starts local Ray in the current Python environment.

The same local flow can be run from the command line with:

```powershell
python -m nbconvert --to notebook --execute 01_download_real_data.ipynb --output 01_download_real_data.local.ipynb
python -m nbconvert --to notebook --execute 02_prepare_assets.ipynb --output 02_prepare_assets.local.ipynb
python -m nbconvert --to notebook --execute 03_run_replay.ipynb --output 03_run_replay.local.ipynb
```

## GitHub Codespaces Docker-Cluster Run

Use this flow when you do not have Docker Desktop on your computer and do not have a separate remote VM. GitHub Codespaces is the Linux environment and Docker host for the demo.

In Codespaces, open a terminal and go to the folder that contains `run_on_docker_engine.sh`.

For the original solution-folder layout:

```bash
cd Ray/4_ray_capstone_project/solution
```

For the flattened Codespaces layout:

```bash
cd /workspaces/-OU-22971-Ray/4_ray_capstone_project
```

Then run:

```bash
docker version
docker compose version || docker-compose version
bash ./run_on_docker_engine.sh --workers 2
```

The same flow can also be run from the notebook:

```text
04_codespaces_docker_launcher.ipynb
```

The Codespaces Docker flow:

1. Verifies Docker in Codespaces with `docker version`, `docker compose version`, and `docker info`.
2. Builds the course Docker image from `Ray/1_cluster_setup/Dockerfile`.
3. Starts the course virtual Ray cluster from `Ray/1_cluster_setup/docker-compose.yml`.
4. Starts one `ray-head` container and the requested number of `ray-worker` containers.
5. Activates the `22971-ray` Conda environment inside `ray-head`.
6. Submits the notebook execution through Ray Jobs from inside `ray-head`.
7. Executes `01_download_real_data.ipynb`, `02_prepare_assets.ipynb`, and `03_run_replay.ipynb` in order with `/opt/conda/envs/22971-ray/bin/python`.
8. Writes durable artifacts under the head container's mounted workspace.

The Docker run writes persistent artifacts here:

```text
Ray/1_cluster_setup/head_workspace/ray_capstone
```

To view the Ray dashboard, wait until `ray-head` starts, then open the Codespaces **Ports** tab and open forwarded port `8265`.

To stop the Docker cluster after the demo:

```bash
cd Ray/1_cluster_setup
docker compose down || docker-compose down
```

## Data Choice

The design doc asks for adjacent monthly Green Taxi files. This solution uses:

- `green_tripdata_2023-01.parquet` as the reference month.
- `green_tripdata_2023-02.parquet` as the replay month.

The preparation notebook uses the first 48 real replay ticks for a practical local and Docker demo run.

## Decision Rule

Each actor snapshot contains current demand, recent mean demand, and the reference baseline for the same zone, hour of day, and day of week.

The scoring task returns `NEED` when:

```text
current demand > max(baseline_count * need_multiplier, recent_mean + 1)
```

Otherwise it returns `OK`.

## Partial-Readiness Policy

Async mode closes a tick when either:

- `completion_fraction` of zones have reported, or
- `tick_timeout_s` expires.

Late or missing zones use `fallback_policy=always_previous`. If a zone has no previous accepted decision, the first fallback is `OK`.

## Output Artifacts

Preparation writes:

- `prepared/active_zones.json`
- `prepared/baseline.parquet`
- `prepared/cross_check.json`
- `prepared/prepare_config.json`
- `prepared/replay_table.parquet`

Replay writes:

- `outputs/notebook_blocking`
- `outputs/notebook_async`
- `outputs/notebook_stress`

Each run folder contains:

- `run_config.json`
- `metrics.csv`
- `latency_log.json`
- `tick_summary.json`
- `decisions.parquet`
- `actor_counters.json`

Stress mode also writes:

- `outputs/notebook_stress/stress_comparison.json`
