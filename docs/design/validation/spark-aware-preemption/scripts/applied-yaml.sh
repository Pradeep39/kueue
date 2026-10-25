# Sourced by the demo runners. Saves every manifest a run applies, so the YAML a part ran with can be
# shown afterwards (`./demo-short.sh yaml N`).
#
# The run's folder is runs/applied/<PART>-<timestamp>/ beside the runner, created on the first apply.
# PART is set by demo-short.sh (part1 ... part6, part6-floorN); a runner started on its own uses its
# own name. Each manifest is saved as NN-<step>.yaml, in the order it was applied.

# Named once, here: record_apply usually runs at the end of a pipeline, in a subshell, so nothing it
# sets survives. It keeps its state in the folder instead.
APPLIED_DIR="$HERE/runs/applied/${PART:-$(basename "$0" .sh)}-$(date +%Y%m%d-%H%M%S)"

# record_apply STEP: saves the manifest on stdin as the next file of this run's folder, then applies it.
# Returns kubectl's exit status.
record_apply() {
  if mkdir -p "$(dirname "$APPLIED_DIR")" && mkdir "$APPLIED_DIR" 2>/dev/null; then
    echo "  YAML applied by this run: $APPLIED_DIR (./demo-short.sh yaml shows it)"
  fi
  local n f
  n=$(find "$APPLIED_DIR" -maxdepth 1 -name '*.yaml' | wc -l | tr -d ' ')
  f=$(printf '%s/%02d-%s.yaml' "$APPLIED_DIR" $((n + 1)) "$1")
  cat > "$f"
  kubectl apply -f "$f" >/dev/null
}
