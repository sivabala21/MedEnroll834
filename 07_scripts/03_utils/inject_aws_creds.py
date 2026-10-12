#!/usr/bin/env python3
"""
inject_aws_creds.py
After `aws login`, export the ACTIVE temporary AWS credentials into .env so the
Airflow containers can use them. Temporary creds (from the new AWS experience)
expire ~12h, so re-run this before a demo run.

Usage:
    aws login                 # (you run this first, in your terminal)
    python 07_scripts/03_utils/inject_aws_creds.py
"""
import re
import subprocess
import sys
from pathlib import Path

ENV = Path(__file__).resolve().parents[2] / ".env"


def _aws(cmd):
    out = subprocess.run(["aws"] + cmd, capture_output=True, text=True, shell=True)
    return out.stdout.strip(), out.stderr.strip(), out.returncode


def get_creds():
    """Resolve the active credentials boto3/CLI would use (incl. session token)."""
    keys = {}
    for name, query in [
        ("AWS_ACCESS_KEY_ID", "AccessKeyId"),
        ("AWS_SECRET_ACCESS_KEY", "SecretAccessKey"),
        ("AWS_SESSION_TOKEN", "SessionToken"),
    ]:
        out, err, rc = _aws(["configure", "export-credentials", "--format", "process"])
        if rc != 0:
            print(f"ERROR running aws configure export-credentials:\n{err}")
            sys.exit(1)
        import json
        data = json.loads(out)
        keys = {
            "AWS_ACCESS_KEY_ID": data.get("AccessKeyId", ""),
            "AWS_SECRET_ACCESS_KEY": data.get("SecretAccessKey", ""),
            "AWS_SESSION_TOKEN": data.get("SessionToken", ""),
        }
        break
    return keys


def set_env(keys):
    text = ENV.read_text(encoding="utf-8")
    for k, v in keys.items():
        pattern = rf"^{k}=.*$"
        line = f"{k}={v}"
        if re.search(pattern, text, flags=re.MULTILINE):
            text = re.sub(pattern, line, text, flags=re.MULTILINE)
        else:
            text += f"\n{line}"
    ENV.write_text(text, encoding="utf-8")


def main():
    keys = get_creds()
    if not keys.get("AWS_ACCESS_KEY_ID"):
        print("No credentials found. Run `aws login` first.")
        return 1
    set_env(keys)
    masked = keys["AWS_ACCESS_KEY_ID"][:4] + "..." + keys["AWS_ACCESS_KEY_ID"][-4:]
    print(f"Injected AWS temp credentials into .env (key {masked}).")
    print("Session token present:", bool(keys.get("AWS_SESSION_TOKEN")))
    print("Reminder: these expire ~12h. Re-run after `aws login` to refresh.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
