#!/bin/bash
# Run an energy-domain report with timestamped log markers + exit code.
cd /root/energy-domain || exit 1
echo "===== START $(date '+%Y-%m-%d %H:%M:%S %Z') : $* ====="
./venv/bin/python "$@"
rc=$?
echo "===== END   $(date '+%Y-%m-%d %H:%M:%S %Z') : exit $rc ====="
echo
