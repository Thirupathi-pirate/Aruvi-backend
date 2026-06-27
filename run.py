import os, sys, subprocess

base = os.path.dirname(os.path.abspath(__file__))
code_run = os.path.join(base, "code", "run.py")

if os.path.exists(code_run):
    os.chdir(os.path.join(base, "code"))
    subprocess.run([sys.executable, code_run])
else:
    print("ERROR: code/run.py not found")
    sys.exit(1)