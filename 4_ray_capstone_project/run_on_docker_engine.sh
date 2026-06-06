#!/usr/bin/env bash
set -euo pipefail

WORKERS=2
RAY_JOBS_ADDRESS="http://127.0.0.1:8265"
CLUSTER_ARTIFACT_ROOT="/workspace/ray_capstone"
CONDA_ENV_NAME="22971-ray"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers|-w)
      WORKERS="$2"
      shift 2
      ;;
    --ray-jobs-address)
      RAY_JOBS_ADDRESS="$2"
      shift 2
      ;;
    --cluster-artifact-root)
      CLUSTER_ARTIFACT_ROOT="$2"
      shift 2
      ;;
    --help|-h)
      cat <<'EOF'
Usage:
  bash ./run_on_docker_engine.sh --workers 2

This runner uses Docker Engine through the docker CLI. It does not require
host-side Conda. It can run in GitHub Codespaces, Google Cloud Shell, or any
Linux host that has Docker Engine and Docker Compose available.
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker CLI was not found. Install/start Docker Engine on this Linux host." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker Engine is not reachable. Start the Docker daemon, then rerun this script." >&2
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE=(docker-compose)
else
  echo "Docker Compose is not available. Install the docker compose plugin or docker-compose, then rerun this script." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -d "$SCRIPT_DIR/../../1_cluster_setup" ]]; then
  CLUSTER_ROOT="$(cd "$SCRIPT_DIR/../../1_cluster_setup" && pwd)"
elif [[ -d "$SCRIPT_DIR/../1_cluster_setup" ]]; then
  CLUSTER_ROOT="$(cd "$SCRIPT_DIR/../1_cluster_setup" && pwd)"
else
  echo "Could not find 1_cluster_setup relative to $SCRIPT_DIR." >&2
  echo "Expected one of:" >&2
  echo "  $SCRIPT_DIR/../../1_cluster_setup" >&2
  echo "  $SCRIPT_DIR/../1_cluster_setup" >&2
  exit 1
fi

HOST_WORKSPACE_ROOT="$CLUSTER_ROOT/head_workspace"
HOST_SUBMISSION_ROOT="$HOST_WORKSPACE_ROOT/ray_capstone_submission"
CONTAINER_SUBMISSION_ROOT="/workspace/ray_capstone_submission"
CONTAINER_PYTHON="/opt/conda/envs/$CONDA_ENV_NAME/bin/python"

JOB_COMMAND="test -x $CONTAINER_PYTHON && echo Running notebooks with $CONTAINER_PYTHON && mkdir -p $CLUSTER_ARTIFACT_ROOT/notebooks $CLUSTER_ARTIFACT_ROOT/data $CLUSTER_ARTIFACT_ROOT/prepared $CLUSTER_ARTIFACT_ROOT/outputs && CAPSTONE_DATA_DIR=$CLUSTER_ARTIFACT_ROOT/data CAPSTONE_PREPARED_DIR=$CLUSTER_ARTIFACT_ROOT/prepared CAPSTONE_OUTPUT_ROOT=$CLUSTER_ARTIFACT_ROOT/outputs RAY_ADDRESS=auto $CONTAINER_PYTHON -m nbconvert --to notebook --execute 01_download_real_data.ipynb --output-dir $CLUSTER_ARTIFACT_ROOT/notebooks --output 01_download_real_data.cluster.ipynb && CAPSTONE_DATA_DIR=$CLUSTER_ARTIFACT_ROOT/data CAPSTONE_PREPARED_DIR=$CLUSTER_ARTIFACT_ROOT/prepared CAPSTONE_OUTPUT_ROOT=$CLUSTER_ARTIFACT_ROOT/outputs RAY_ADDRESS=auto $CONTAINER_PYTHON -m nbconvert --to notebook --execute 02_prepare_assets.ipynb --output-dir $CLUSTER_ARTIFACT_ROOT/notebooks --output 02_prepare_assets.cluster.ipynb && CAPSTONE_DATA_DIR=$CLUSTER_ARTIFACT_ROOT/data CAPSTONE_PREPARED_DIR=$CLUSTER_ARTIFACT_ROOT/prepared CAPSTONE_OUTPUT_ROOT=$CLUSTER_ARTIFACT_ROOT/outputs RAY_ADDRESS=auto $CONTAINER_PYTHON -m nbconvert --to notebook --execute 03_run_replay.ipynb --output-dir $CLUSTER_ARTIFACT_ROOT/notebooks --output 03_run_replay.cluster.ipynb"

show_docker_info() {
  echo
  echo "Docker evidence for the live demo"
  echo "---------------------------------"
  docker version
  "${DOCKER_COMPOSE[@]}" version
  docker info --format 'Engine={{.ServerVersion}}; OS={{.OperatingSystem}}; OSType={{.OSType}}; CPUs={{.NCPU}}'
  echo
}

copy_submission_files() {
  mkdir -p "$HOST_SUBMISSION_ROOT"
  cp -f "$SCRIPT_DIR/01_download_real_data.ipynb" "$HOST_SUBMISSION_ROOT/"
  cp -f "$SCRIPT_DIR/02_prepare_assets.ipynb" "$HOST_SUBMISSION_ROOT/"
  cp -f "$SCRIPT_DIR/03_run_replay.ipynb" "$HOST_SUBMISSION_ROOT/"
  if [[ -f "$SCRIPT_DIR/README.md" ]]; then
    cp -f "$SCRIPT_DIR/README.md" "$HOST_SUBMISSION_ROOT/"
  fi
}

wait_for_ray_head() {
  echo "Waiting for ray-head to become healthy..."
  for _ in $(seq 1 60); do
    health="$(docker inspect -f '{{.State.Health.Status}}' ray-head 2>/dev/null || true)"
    if [[ "$health" == "healthy" ]]; then
      echo "ray-head is healthy."
      return 0
    fi
    sleep 2
  done

  "${DOCKER_COMPOSE[@]}" ps
  echo "ray-head did not become healthy. Check logs with: ${DOCKER_COMPOSE[*]} logs ray-head" >&2
  exit 1
}

copy_submission_files

pushd "$CLUSTER_ROOT" >/dev/null
show_docker_info
"${DOCKER_COMPOSE[@]}" build
"${DOCKER_COMPOSE[@]}" up -d --scale "ray-worker=$WORKERS"
wait_for_ray_head

"${DOCKER_COMPOSE[@]}" ps
docker ps --filter "name=ray" --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
docker exec ray-head bash -lc "hostname && source /opt/conda/etc/profile.d/conda.sh && conda activate $CONDA_ENV_NAME && echo Conda env: \$CONDA_DEFAULT_ENV && which python && python -m nbconvert --version && ray status"

docker exec ray-head bash -lc "source /opt/conda/etc/profile.d/conda.sh && conda activate $CONDA_ENV_NAME && ray job submit --address $RAY_JOBS_ADDRESS --working-dir $CONTAINER_SUBMISSION_ROOT -- bash -lc '$JOB_COMMAND'"
popd >/dev/null
