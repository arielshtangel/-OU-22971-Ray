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

Run the capstone Python scripts on the course Docker-based Ray cluster.
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
  echo "Docker CLI was not found." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker Engine is not reachable." >&2
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE=(docker-compose)
else
  echo "Docker Compose is not available." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -d "$SCRIPT_DIR/../1_cluster_setup" ]]; then
  CLUSTER_ROOT="$(cd "$SCRIPT_DIR/../1_cluster_setup" && pwd)"
elif [[ -d "$SCRIPT_DIR/../../1_cluster_setup" ]]; then
  CLUSTER_ROOT="$(cd "$SCRIPT_DIR/../../1_cluster_setup" && pwd)"
else
  echo "Could not find 1_cluster_setup relative to $SCRIPT_DIR." >&2
  exit 1
fi

HOST_WORKSPACE_ROOT="$CLUSTER_ROOT/head_workspace"
HOST_SUBMISSION_ROOT="$HOST_WORKSPACE_ROOT/ray_capstone_submission"
CONTAINER_SUBMISSION_ROOT="/workspace/ray_capstone_submission"
CONTAINER_PYTHON="/opt/conda/envs/$CONDA_ENV_NAME/bin/python"

JOB_COMMAND="test -x $CONTAINER_PYTHON && echo Running scripts with $CONTAINER_PYTHON && mkdir -p $CLUSTER_ARTIFACT_ROOT/data $CLUSTER_ARTIFACT_ROOT/prepared $CLUSTER_ARTIFACT_ROOT/outputs && $CONTAINER_PYTHON prepare.py --data-dir $CLUSTER_ARTIFACT_ROOT/data --output-dir $CLUSTER_ARTIFACT_ROOT/prepared --reference-month 2023-01 --replay-month 2023-02 --n-zones 6 --tick-minutes 15 --max-ticks 48 && RAY_ADDRESS=auto $CONTAINER_PYTHON run.py --prepared-dir $CLUSTER_ARTIFACT_ROOT/prepared --output-dir $CLUSTER_ARTIFACT_ROOT/outputs --mode all --ray-address auto"

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
  cp -f "$SCRIPT_DIR/prepare.py" "$HOST_SUBMISSION_ROOT/"
  cp -f "$SCRIPT_DIR/run.py" "$HOST_SUBMISSION_ROOT/"
  if [[ -f "$SCRIPT_DIR/README.md" ]]; then
    cp -f "$SCRIPT_DIR/README.md" "$HOST_SUBMISSION_ROOT/"
  fi
  if [[ -f "$SCRIPT_DIR/../README.md" ]]; then
    cp -f "$SCRIPT_DIR/../README.md" "$HOST_SUBMISSION_ROOT/"
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
docker exec ray-head bash -lc "hostname && source /opt/conda/etc/profile.d/conda.sh && conda activate $CONDA_ENV_NAME && echo Conda env: \$CONDA_DEFAULT_ENV && which python && ray status"

docker exec ray-head bash -lc "source /opt/conda/etc/profile.d/conda.sh && conda activate $CONDA_ENV_NAME && ray job submit --address $RAY_JOBS_ADDRESS --working-dir $CONTAINER_SUBMISSION_ROOT -- bash -lc '$JOB_COMMAND'"
popd >/dev/null
