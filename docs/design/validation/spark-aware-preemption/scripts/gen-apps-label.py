#!/usr/bin/env python3
"""gen-apps.py plus --label KEY=VALUE (repeatable), added to each SparkApplication's labels.

  gen-apps-label.py NAME:PRIORITY[,...] [--label k=v ...] [gen-apps.py options]
"""
import os
import subprocess
import sys

args, labels = [], []
it = iter(sys.argv[1:])
for a in it:
    if a == "--label":
        labels.append(next(it))
    else:
        args.append(a)
out = subprocess.run([sys.executable, "-I", os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen-apps.py"), *args],
                     check=True, capture_output=True, text=True).stdout
extra = "".join(f'\n    {k}: "{v}"' for k, v in (l.split("=", 1) for l in labels))
print(out.replace("  labels:\n    kueue.x-k8s.io/queue-name:", "  labels:" + extra + "\n    kueue.x-k8s.io/queue-name:"), end="")
