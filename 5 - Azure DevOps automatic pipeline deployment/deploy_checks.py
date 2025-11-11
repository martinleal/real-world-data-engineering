"""Deploy a list of Check configuration files sequentially via a single Azure DevOps
pipeline that supports parameters for check deployment.

FOCUS: Only deploy check rules (doDeployCheckRules=true). Other deployment
capabilities are intentionally omitted for now.

Usage examples (PowerShell):
  # Deploy checks from JSON file sequentially (waits for completion of each)
  $env:AZDO_ORG_URL = "YOUR_ORG_URL"
  $env:AZDO_PROJECT = "YOUR_PROJECT_NAME"
  $env:AZDO_PAT = "***"
  python .\deploy_checks.py --config .\checks_to_deploy.json

checks_to_deploy.json example:
  {
    "pipeline": 164,
    "branch": "develop",
    "env": "PRE",
    "checks": [
      "/config/config_checks/DIM_PLAN_ACTION_EVIDENCE_STATUS/CONSISTENCY__SOSTENIBILIDAD__DHWSOSTENIBILIDAD_DATA__DIM_PLAN_ACTION_EVIDENCE_STATUS.yaml",
      "/config/config_checks/ANOTHER_CHECK.yaml"
    ]
  }

It will wait for each run to finish (default) and produce a summary table.
Return code: 0 if all succeeded; 1 if any failed; 2 for usage/env errors.
"""
from __future__ import annotations
import os
import sys
import time
import json
import argparse
import base64
from typing import Dict, Any, List
import requests

API_VERSION = "7.1-preview.1"
DEFAULT_POLL_INTERVAL = 15

class DeployChecksError(Exception):
    pass

def get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise DeployChecksError(f"Missing environment variable {name}")
    return v

def headers_from_pat(pat: str) -> Dict[str, str]:
    token = f":{pat}"  # Basic auth with empty username
    b64 = base64.b64encode(token.encode()).decode()
    return {"Authorization": f"Basic {b64}", "Content-Type": "application/json"}

def trigger_run(org_url: str, project: str, pipeline_id: int, branch: str, parameters: Dict[str, Any], headers: Dict[str, str], debug: bool=False) -> Dict[str, Any]:
    url = f"{org_url}/{project}/_apis/pipelines/{pipeline_id}/runs?api-version={API_VERSION}"
    payload: Dict[str, Any] = {
        "resources": {"repositories": {"self": {"refName": branch}}},
        "parameters": parameters,
        "templateParameters": parameters,  # maximize compatibility
    }
    if debug:
        print("[debug] Payload:" + json.dumps(payload, indent=2))
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code >= 300:
        raise DeployChecksError(f"Trigger failed ({r.status_code}): {r.text}")
    return r.json()

def get_run(org_url: str, project: str, pipeline_id: int, run_id: int, headers: Dict[str, str]) -> Dict[str, Any]:
    url = f"{org_url}/{project}/_apis/pipelines/{pipeline_id}/runs/{run_id}?api-version={API_VERSION}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code >= 300:
        raise DeployChecksError(f"Get run failed ({r.status_code}): {r.text}")
    return r.json()

def wait(org_url: str, project: str, pipeline_id: int, run_id: int, headers: Dict[str, str], poll: int) -> str:
    while True:
        data = get_run(org_url, project, pipeline_id, run_id, headers)
        state = data.get("state")
        result = data.get("result")
        if state == "completed":
            return result or "unknown"
        time.sleep(poll)

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sequential deployment of check rules via Azure DevOps pipeline")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--check-file", action="append", help="Path to a check YAML file (repeatable)")
    g.add_argument("--config", help="JSON config file with pipeline, env, branch, checks[]")
    p.add_argument("--pipeline", type=int, help="Pipeline definition id (if not in config)")
    p.add_argument("--env", help="Environment value for parameter 'env' (if not in config)")
    p.add_argument("--branch", default="develop", help="Branch/ref to run (default develop)")
    p.add_argument("--check-folder", default="/config/check_queries", help="Parameter check_folder override")
    p.add_argument("--interval", type=int, default=DEFAULT_POLL_INTERVAL, help="Polling interval seconds")
    p.add_argument("--no-wait", action="store_true", help="Do not wait for completion; just trigger sequentially")
    p.add_argument("--json", action="store_true", help="Emit JSON summary")
    p.add_argument("--debug", action="store_true", help="Print debug payloads")
    return p.parse_args(argv)

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_ref(ref: str) -> str:
    if ref.startswith("refs/"):
        return ref
    return "refs/heads/" + ref

def main(argv: List[str]) -> int:
    args = parse_args(argv)
    try:
        org_url = get_env("AZDO_ORG_URL")
        project = get_env("AZDO_PROJECT")
        pat = get_env("AZDO_PAT")
    except DeployChecksError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    headers = headers_from_pat(pat)

    if args.config:
        cfg = load_config(args.config)
        pipeline_id = int(cfg.get("pipeline")) if cfg.get("pipeline") is not None else args.pipeline
        env_value = cfg.get("env") or args.env
        branch = cfg.get("branch") or args.branch
        checks = cfg.get("checks") or []
    else:
        pipeline_id = args.pipeline
        env_value = args.env
        branch = args.branch
        checks = args.check_file or []

    if not pipeline_id:
        print("ERROR: Missing pipeline id (use --pipeline or config)", file=sys.stderr)
        return 2
    if not env_value:
        print("ERROR: Missing env value (use --env or config)", file=sys.stderr)
        return 2
    if not checks:
        print("ERROR: No checks supplied", file=sys.stderr)
        return 2

    ref = normalize_ref(branch)
    print(f"Deploying {len(checks)} check(s) sequentially via pipeline {pipeline_id} on {ref} env={env_value}")

    summary = []
    errors = 0
    for idx, check_path in enumerate(checks, start=1):
        # Force all other deployment toggles to False to avoid accidental activation
        params = {
            "env": env_value,
            "doDeploySnowflakeCICD": False,
            "doDeployDG": False,
            "doDeployAlerts": False,
            "doDeployCheckRules": True,
            "check_file": check_path,
            "check_folder": args.check_folder,
        }
        print(f"[{idx}/{len(checks)}] Triggering check: {check_path}")
        try:
            data = trigger_run(org_url, project, pipeline_id, ref, params, headers, debug=args.debug)
            run_id = data.get("id")
            print(f"    Run id: {run_id}")
            result = None
            if not args.no_wait:
                result = wait(org_url, project, pipeline_id, run_id, headers, args.interval)
                print(f"    Result: {result}")
            summary.append({"check_file": check_path, "run_id": run_id, "result": result})
            if result and result != "succeeded":
                errors += 1
        except DeployChecksError as e:
            print(f"    ERROR: {e}", file=sys.stderr)
            summary.append({"check_file": check_path, "run_id": None, "error": str(e)})
            errors += 1

    # If no_wait, fill result=None for triggered runs
    if args.json:
        print(json.dumps({"summary": summary, "errors": errors}, indent=2))
    else:
        print("\nSummary:")
        for row in summary:
            status = row.get("result") or row.get("error") or "pending"
            print(f" - {row['check_file']} -> {status}")
        if errors:
            print(f"Failures: {errors}")
        else:
            print("All checks deployed successfully")

    return 0 if errors == 0 else 1

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
