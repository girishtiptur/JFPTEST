 

import argparse
import asyncio
import os
import platform
import shutil
import subprocess
from datetime import datetime
from typing import Optional
from types import SimpleNamespace

import yaml

import monitoring_logging
import monitoring_manifest
from jfp_utils import JfpCliUtils
from jfp_slurmrest_utils import JfpSlurmUtils
from cluster_utils import (add_verbose_output_arg, CustomHelpFormatter, remote_exists, remote_ls, remote_open, Systemd,
                           check_ping, remote_replace_file_contents, remote_delete)
from zookeeper_tools import async_cluster_mutex

logger = monitoring_logging.get_trace_logger("monitoring_management." + __name__)

# importing jfp_utils and jfp_slurm_utils

JFP_UTIL = JfpCliUtils()
JFP_SLURM_UTIL = JfpSlurmUtils()

PDSH = "/opt/clmgr/bin/pdsh"


################################################# ########################
# hepers function for JFP setup

# Start JFP Services
def start_jfp_services(node):
    if not JFP_UTIL.is_jfp_running(node):
        JFP_UTIL.enable_jfp_services(node)
        JFP_UTIL.start_jfp_services(node)

    # Stop JFP Services


def stop_jfp_services(node):
    JFP_UTIL.stop_jfp_services(node)
    JFP_UTIL.disable_jfp_services(node)


# Initialize and verify all dependencies required for JFP setup
async def jfp_setup(node, skip_services_check=False):
    """
    Initializes and verifies all necessary dependencies for JFP setup on the given node.
    
    Steps include:
    1. (Optional) Monitoring services check (Kafka, OpenSearch)
    2. Slurm REST API setup and validation:
        - Configuration setup
        - Installation verification
        - API request/response validation
        - Data collector setup
    3. JFP-specific initialization
    4. Image presence check and loading (idempotent)
    """
    # RPM build is supported ,to check image and tar.gz exists.
    JFP_UTIL.check_jfp_installed(node)

    # Verify Kafka and OpenSearch configurations.
    if not skip_services_check:
        await JFP_UTIL.monitoring_services_check(check_running=True, check_enabled=True)

        # JFP checks for slurmrest -: below 2 checks
        # 1) slurmrest install setup 2) collector setup

    # Verify Slurm REST API setup configurations.
    if not skip_services_check:
        JFP_UTIL.slurm_rest_api_setup_check(node)
    
        # Verify Slurm REST API data collector setup configurations.
    if not skip_services_check:
        JFP_UTIL.slurm_rest_api_collector_check(node)

    # Initialize JFP-specific setup scripts
    await JFP_UTIL.initial_jfp_setup(node)

    # Initialize image if not already present (idempotent)
    jfp_image_tar = JFP_UTIL.get_jfp_image_tar(node)
    jfp_image_name = JFP_UTIL.get_jfp_image_name(node)
    JFP_UTIL.load_image(jfp_image_tar, jfp_image_name, node)


##################################################################### 
# Monitoring JFP setup functions

@async_cluster_mutex("monitoring_kafka")
async def setup(node: Optional[str] = None, skip_services_check: bool = False) -> None:
    """
    Setup/validate JFP on the given node (idempotent; no file moves).      
    - If repo YAMLs are not present, alert.
    - If services are not running AND repo YAMLs exist, start services 
    - Optionally add images.
    - Add dashboards.
    
    """
    logger.info("setting up JFP...\n")

    ACTION = "action: Please re-run training setup: cm monitoring jfp setup trainer --dur=7"

    # 1) Reachability
    if not check_ping("localhost", node):
        print(f"[JFP setup] abort: node not reachable [node={node}]", flush=True)
        return

    label = "admin" if node == "localhost" else node

    # 2)  # Main function checks all initial check 
    await jfp_setup(node=node, skip_services_check=skip_services_check)

    model_base = JFP_UTIL.model_path
    metadata_base = JFP_UTIL.metadata_path

    # Discover files early
    jfp_models = JFP_UTIL.get_jfp_model_repo(dir=model_base, node=node)
    metadata_files = JFP_UTIL.get_models(dir=metadata_base, node=node) or []

    # Helper: remote-safe existence via dir listing
    def _exists_via_ls(abs_path: str, n) -> bool:
        if not abs_path:
            return False
        d, f = os.path.dirname(abs_path), os.path.basename(abs_path)
        try:
            entries = JFP_UTIL.get_models(dir=d, node=n) or []
        except Exception:
            return False
        return f in set(entries)

    # 3) Ensure repo YAML exists
    if not jfp_models:
        print(
            f"[JFP setup] no model repository YAML in model_path[{model_base}]; expected: model_repository*.yaml; {ACTION}",
            flush=True)
        print("[JFP setup] First model builds after 7 [default] days of history.", flush=True)
        print(f"From day 8, enable JFP via command. Until then it won't start. {ACTION}", flush=True)
        return

    repo_fname = jfp_models[0]
    model_repo_path = os.path.join(model_base, repo_fname)
    if not _exists_via_ls(model_repo_path, node):
        print(f"[JFP setup] repository YAML missing [{model_repo_path}]; {ACTION}", flush=True)
        return

    # 4) Load YAML + normalize operators
    try:
        repo_cfg = JFP_UTIL.load_yaml(model_repo_path, node=node)
    except Exception as e:
        print(f"[JFP setup] parse error [{model_repo_path}]; error: {e}; {ACTION}", flush=True)
        return

    def _extract_operators(cfg) -> list:
        if isinstance(cfg, dict):
            ops = cfg.get("operators")
            return ops if isinstance(ops, list) else []
        if isinstance(cfg, list):
            for item in cfg:
                if isinstance(item, dict) and isinstance(item.get("operators"), list):
                    return item["operators"]
            if all(isinstance(x, dict) for x in cfg):
                return cfg  # already operators
        return []

    operators = _extract_operators(repo_cfg)
    if not operators:
        print(f"[JFP setup] invalid YAML: no 'operators' [{model_repo_path}]; {ACTION}", flush=True)
        return

    # 5) Extract encoder/model paths (pkl/ubj) #ToDo check weather we need PKL or string based features set available
    encoder_path: Optional[str] = None
    model_path_from_repo: Optional[str] = None
    for op in operators:
        if not isinstance(op, dict):
            continue
        if isinstance(op.get("encoder_path"), str):
            encoder_path = op["encoder_path"]
        params = op.get("parameters")
        if isinstance(params, dict):
            if isinstance(params.get("encoder_path"), str) and not encoder_path:
                encoder_path = params["encoder_path"]
            if isinstance(params.get("model_path"), str):
                model_path_from_repo = params["model_path"]

    if not model_path_from_repo:
        print(f"[JFP setup] missing UBJ path (operators?parameters.model_path) in YAML [{model_repo_path}]; {ACTION}",
              flush=True)
        return
    if not encoder_path:
        print(f"[JFP setup] missing PKL path (operators?encoder_path) in YAML [{model_repo_path}]; {ACTION}",
              flush=True)
        return

    # 6) Verify UBJ/PKL present
    ubj_ok = _exists_via_ls(model_path_from_repo, node)
    pkl_ok = _exists_via_ls(encoder_path, node)
    if not ubj_ok and not pkl_ok:
        print(f"[JFP setup] artifacts missing (ubj & pkl) [ubj={model_path_from_repo} pkl={encoder_path}]; {ACTION}",
              flush=True)
        return
    if not ubj_ok:
        print(f"[JFP setup] ubj not found [{model_path_from_repo}]; {ACTION}", flush=True)
        return
    if not pkl_ok:
        print(f"[JFP setup] pkl not found [{encoder_path}]; {ACTION}", flush=True)
        return

    # 7) Metadata REQUIRED (as per your latest ask) ? if empty, STOP
    if len(metadata_files) == 0:
        print(f"[JFP setup] metadata missing [dir={metadata_base}]; action: please ensure metadata exists", flush=True)
        return

    # Helper to re-check artifacts before success prints
    def _core_assets_ok() -> tuple[bool, list[str]]:
        missing = []
        if not _exists_via_ls(model_path_from_repo, node):
            missing.append(f"ubj={model_path_from_repo}")
        if not _exists_via_ls(encoder_path, node):
            missing.append(f"pkl={encoder_path}")
        return (len(missing) == 0), missing

    # 8) Service state & messages
    already_running = False if skip_services_check else bool(JFP_UTIL.is_jfp_running(node=node, any_running=False))

    if already_running:
        ok, missing = _core_assets_ok()
        if ok:
            print(
                f"[JFP setup] note: JFP service already running [node={label}]\n"
                f"[JFP setup] model/artifacts ok [ubj={model_path_from_repo} pkl={encoder_path}]",
                flush=True
            )
        else:
            print(
                f"[JFP setup] note: JFP service running but model/artifacts missing [node={label}] [{', '.join(missing)}]; {ACTION}",
                flush=True)
    else:
        start_jfp_services(node=node)
        healthy = True if skip_services_check else bool(JFP_UTIL.is_jfp_running(node=node, any_running=False))

        ok, missing = _core_assets_ok()
        if healthy and ok:
            print(f"[JFP setup] activated [node={label}]", flush=True)
            print(f"[JFP setup] model/artifacts verified [ubj={model_path_from_repo} pkl={encoder_path}]", flush=True)

        elif healthy and not ok:
            print(f"[JFP setup] activated with missing artifacts [node={label}]", flush=True)
            print(f"[JFP setup] missing: {', '.join(missing)}; {ACTION}", flush=True)

        else:
            print(f"[JFP setup] services not healthy after start [node={label}]", flush=True)
            print("[JFP setup] hint: check logs with `journalctl -u jfp_service -e`", flush=True)

    # 9) Dashboards + manifest
    dashboards = JFP_UTIL.common_dashboards
    count = len(dashboards) if hasattr(dashboards, "__len__") else "N/A"
    # print(f"[JFP setup] dashboards+manifest updating [dashboards={count} node={label}]", flush=True)
    JFP_UTIL.add_dashboards(dashboards=dashboards, node=node)
    await monitoring_manifest.set_host_with_jfp_setup(val=node)


############################################################################################################################################   
# Monitoring JFP teardown functions

@async_cluster_mutex("monitoring_kafka")
async def teardown(delete_images: bool = False) -> None:
    """
    Tear down JFP on the configured node (no file moves).   
    - If services are running AND repo YAMLs exist, stop services and clear manifest.
    - Optionally delete images.
    - Remove dashboards.
    """
    logger.info("tearing down JFP...")

    node = await monitoring_manifest.get_host_with_jfp_setup()
    if node == "":
        print("JFP is not setup. It can be setup with 'cm monitoring jfp setup'.")
        return

    label = "admin" if node == "localhost" else node
    
    
    #call trainer teardown and slumrest collector teardown if enabled
     
    setup_yn = await monitoring_manifest.get_jfp_trainer_enabled()    
    if setup_yn:
        print("[JFP teardown] JFP trainer should be teardown using command:'cm monitoring jfp teardown trainer'")
        return
    
    #check since images to be palced
    JFP_UTIL.check_jfp_installed(node)

    # (Optional) paths / listings
    model_path = JFP_UTIL.model_path
    running_models = JFP_UTIL.get_models(dir=model_path, node=node) or []

    # Alert if model_repository YAMLs are present
    repo_yamls = [
        m for m in running_models
        if m.startswith("model_repository") and m.endswith(".yaml")
    ]

    # Stop JFP services only if running AND repo YAMLs exist
    if JFP_UTIL.is_jfp_running(node) and repo_yamls:
        print("\n")
        print(f"[JFP teardown] JFP services detected running on node={label}; stopping JFP services...")
        stop_jfp_services(node=node)
        await monitoring_manifest.set_host_with_jfp_setup(val="")

            
    # Optionally delete images
    if delete_images:
        jfp_image_name = JFP_UTIL.get_jfp_image_name(node)
        JFP_UTIL.delete_image(jfp_image_name, node)

    # Remove dashboards
    dashboards = JFP_UTIL.common_dashboards
    JFP_UTIL.remove_dashboards(dashboards=dashboards, node=node)
   
    setupcol_yn = await monitoring_manifest.get_jfp_slurmrest_enabled()
    if setupcol_yn:         
        print(f"[JFP teardown Info] Slurm REST collector should be teardown using command:'cm monitoring jfp teardown slurmrest-col'")
        


############################################################################################################################################
# Monitoring JFP restart functions

async def run_restart_op():
    """Restart JFP services"""
    node = await monitoring_manifest.get_host_with_jfp_setup()

    if not node:
        print("JFP is not setup. It can be setup with 'cm monitoring jfp setup'")
        return

    print("Restarting JFP services...")
    stop_jfp_services(node)
    start_jfp_services(node)
    print("JFP services restarted.")


##########################################################################################################################
# Monitoring JFP status functions

async def status() -> str:
    """Get status of JFP monitoring, with artifact paths from repo YAMLs (shown only when running)."""
    try:
        containers = JFP_UTIL.jfp_containers
        node = await monitoring_manifest.get_host_with_jfp_setup()

        if not node:
            return "JFP is not setup. It can be setup with 'cm monitoring jfp setup'."

        pnode = node if node != "localhost" else "admin"

        # --- Collect container statuses
        statuses = []
        running_count = 0
        for c in containers:
            s = (JFP_UTIL.containerStatus(c, node) or "").strip().lower()
            s = s if s else "unknown"
            statuses.append((c, s))
            if s == "running":
                running_count += 1

        total = len(containers)
        all_running = (running_count == total and total > 0)
        any_running = (running_count > 0)

        # --- Helpers to extract artifacts (same as before)
        def _coerce_str(x):
            return x.strip() if isinstance(x, str) else None

        def _first_key(d: dict, keys):
            for k in keys:
                if k in d and isinstance(d[k], str) and d[k].strip():
                    return d[k].strip()
            return None

        def _deep_find_path(obj, want_exts):
            stack = [obj]
            while stack:
                cur = stack.pop()
                if isinstance(cur, dict):
                    stack.extend(cur.values())
                elif isinstance(cur, list):
                    stack.extend(cur)
                elif isinstance(cur, str):
                    s = cur.strip()
                    low = s.lower()
                    for ext in want_exts:
                        if low.endswith(ext):
                            return s
            return None

        def _extract_artifacts_from_cfg(cfg):
            ubj_keys = ["model_path", "ubj", "model", "artifact", "model_file", "model_uri", "path"]
            pkl_keys = ["encoder_path", "pkl", "encoder", "preprocessor", "vectorizer", "encoder_uri"]
            if isinstance(cfg, dict):
                ubj = _first_key(cfg, ubj_keys)
                pkl = _first_key(cfg, pkl_keys)
                if ubj or pkl:
                    return (_coerce_str(ubj), _coerce_str(pkl))
            operators = []
            if isinstance(cfg, dict) and isinstance(cfg.get("operators"), list):
                operators = cfg["operators"]
            elif isinstance(cfg, list) and all(isinstance(x, dict) for x in cfg):
                operators = cfg
            for op in operators:
                if not isinstance(op, dict):
                    continue
                ubj = _first_key(op, ubj_keys)
                pkl = _first_key(op, pkl_keys)
                params = None
                for pkey in ("parameters", "params", "config", "settings", "args"):
                    if isinstance(op.get(pkey), dict):
                        params = op[pkey]
                        break
                if params:
                    ubj = ubj or _first_key(params, ubj_keys)
                    pkl = pkl or _first_key(params, pkl_keys)
                if ubj or pkl:
                    return (_coerce_str(ubj), _coerce_str(pkl))
            ubj_exts = (".ubj", ".ubj.zst", ".ubj.gz", ".onnx", ".bin")
            pkl_exts = (".pkl", ".joblib", ".pickle")
            ubj = _deep_find_path(cfg, ubj_exts)
            pkl = _deep_find_path(cfg, pkl_exts)
            return (_coerce_str(ubj), _coerce_str(pkl))

        # --- Determine state for headline
        if all_running:
            state = "RUNNING"
            headline = "JFP setup status: Running"
        elif any_running:
            state = "DEGRADED"
            headline = "JFP setup status: Degraded"
        else:
            state = "STOPPED"
            headline = "JFP setup status: Stopped"  # (fixed spelling)

        # --- Build output
        lines = [headline, f"\nNode: {pnode}", "\nJFP services:"]
        for c, s in statuses:
            lines.append(f"\t{c}: {s}")

        # Only show Activated models when fully running
        if state == "RUNNING":
            model_base = JFP_UTIL.model_path
            files = JFP_UTIL.get_models(model_base, node) or []
            repo_yamls = [f for f in files if f.startswith("model_repository") and f.endswith(".yaml")]

            # Collect (repo_name, ubj, pkl, hint) for tidy, two-line output per repo
            activated_details = []
            for fname in repo_yamls:
                repo_path = os.path.join(model_base, fname)
                try:
                    cfg = JFP_UTIL.load_yaml(repo_path, node=node)
                    ubj, pkl = _extract_artifacts_from_cfg(cfg)
                    if ubj or pkl:
                        activated_details.append((fname, ubj, pkl, None))
                    else:
                        top_keys = list(cfg.keys())[:8] if isinstance(cfg, dict) else []
                        activated_details.append((fname, None, None, f"no ubj/pkl found; keys={top_keys}"))
                except Exception as e:
                    activated_details.append((fname, None, None, f"error parsing: {e}"))

            if activated_details:
                lines.append("\nActivated [JFP models]:")
                for repo_name, ubj, pkl, hint in activated_details:
                    # line 1: repo yaml
                    lines.append(f"\tfrom {repo_name}")
                    # line 2: artifact paths (or hint)
                    if hint:
                        lines.append(f"\t  {hint}")
                    else:
                        lines.append(f"\t  ubj={ubj or 'NA'}  pkl={pkl or 'NA'}")
            else:
                lines.append("\nActivated [JFP models]: none detected")

        # Notes based on state
        if state == "DEGRADED":
            lines.append("\nnote: at least one service is not running")
        if state == "RUNNING":
            # no extra note; keep it clean
            pass

        return "\n".join(lines)

    except Exception as e:
        return f"JFP setup status: ERROR\nreason: {e}"


#########################################################################################################################
# Monitoring JFP trainer helpers functions

def load_yaml_file(file_name, node):
    """Loads yaml file"""
    config = {}
    if not remote_exists(node, file_name):
        raise Exception(f"File does not exist: {file_name}")

    with remote_open(node, file_name) as f:
        config = yaml.safe_load("\n".join(f.find_all(".*")))
    return config


def update_trainer_config_files(train_dur, node):
    """Updates trainer config yaml files with training duration"""
    train_config_path = JFP_UTIL.train_config_path
    train_config_files = remote_ls(node, train_config_path)

    for config_file in train_config_files:
        if os.path.isdir(config_file) or not config_file.endswith('.yaml') or \
                config_file.startswith('it') or config_file.startswith('mlflow'):
            continue

        abs_path = os.path.join(train_config_path, config_file)
        configs = load_yaml_file(abs_path, node)

        if 'dates_interval' not in configs['runner'].keys():
            continue

        configs['runner']['dates_interval'] = train_dur
        remote_replace_file_contents(abs_path, yaml.safe_dump(configs), node)


def get_trainer_cron_line(trainer_cron, node):
    """Reads trainer cron file and fetches the cron job line"""
    cron_line = None
    with remote_open(node, trainer_cron) as f:
        for line in f.find_all(".*"):
            if "jfp_trainer_scheduler" in line:
                cron_line = line
    return cron_line


def get_active_trainers(trainer_cron, node):
    """Fetches the list of trainers specified in jfp_trainer cron file"""
    trainers = ['xtime_xgboost']
    cron_line = get_trainer_cron_line(trainer_cron=trainer_cron, node=node)
    cron_trainers = cron_line.split("--trainers")[-1].split()
    return trainers if 'all' in cron_trainers else cron_trainers


def update_trainer_cron(trainer_cron, active_trainers, new_trainer, num_days, node):
    """Updates cron job by adding specified trainers"""
    # trainers = ['jfp_trainer']
    trainers = ['xtime_xgboost']
    train_config_path = JFP_UTIL.train_config_path

    if new_trainer == 'all':
        active_trainers = trainers
    else:
        if new_trainer not in active_trainers:
            active_trainers.append(new_trainer)

    if 'cooldev' in active_trainers:
        update_trainer_config_files(train_dur=num_days, node=node)

    active_trainers_str = ' '.join(active_trainers)

    # Lines to be written into jfp trainer cron file
    cron_entries = [f"# Run trainer once every {num_days} days",
                    f"0 0 * * * root {train_config_path}/jfp_trainer_scheduler "
                    f"--trainers {active_trainers_str}"]

    remote_replace_file_contents(trainer_cron, "\n".join(cron_entries), node)


def update_trainer_status(node, dur=None):
    """Update trainer status file"""
    status_yaml = JFP_UTIL.status_yaml
    status_configs = load_yaml_file(status_yaml, node)

    if dur is not None:
        status_configs['enable']['enabled_date'] = datetime.now().strftime("%Y-%m-%d")
        status_configs['enable']['dur'] = dur
    else:
        # Clear values for teardown
        enable = status_configs['enable']
        status_configs['enable'] = enable.fromkeys(enable)
        run = status_configs['run']
        status_configs['run'] = run.fromkeys(run)
        status_configs['enable']['enabled_date'] = datetime.now().strftime("%Y-%m-%d")

    remote_replace_file_contents(status_yaml, yaml.safe_dump(status_configs), node)


##########################################################################################################################
# Monitoring JFP Trainer helpers functions
cron_dir = JFP_UTIL.cron_dir
trainer_cron = JFP_UTIL.trainer_cron
status_yaml = JFP_UTIL.status_yaml
pdsh = "pdsh"  # adjust if you wrap/alias pdsh


def _safe_get_distro(node: str) -> str:
    """Return 'sles' or 'rhel' (fallback) without throwing."""
    try:
        if node == "localhost":
            out = subprocess.check_output(
                "/opt/oscar/scripts/distro-query | sed -n '/^compat /s/^compat.*: //p'",
                shell=True,
            ).decode("utf-8", errors="ignore")
        else:
            proc = subprocess.run(
                [pdsh, "-N", "-w", node,
                 "/opt/oscar/scripts/distro-query | sed -n '/^compat /s/^compat.*: //p'", "-print", "-quit"],
                capture_output=True, text=True
            )
            out = proc.stdout or ""
        val = (out.split("\n")[0] or "").strip().lower()
        if val in ("sles", "suse", "sles15", "sles12"):
            return "sles"
    except Exception:
        pass
    return "rhel"  # default for crond.service


def _restart_cron_service(node: str) -> None:
    print("Restarting cron service")
    distro = _safe_get_distro(node)
    unit = "cron.service" if distro == "sles" else "crond.service"
    Systemd.restart(node, unit)


def _mlflow_ensure_started(node: str) -> None:
    # idempotent ops; no pre-checks needed
    Systemd.enable(node, "jfp-mlflow.service")
    Systemd.start(node, "jfp-mlflow.service")


def _mlflow_stop_if_idle(node: str) -> None:
    """Always stop/disable mlflow (ignore models)."""
    Systemd.stop(node, "jfp-mlflow.service")
    Systemd.disable(node, "jfp-mlflow.service")


def _cron_present(node: str) -> bool:
    return bool(remote_exists(node, trainer_cron))


def _print_enabled_status_header(node: str) -> None:
    pnode = node if node != "localhost" else "admin"
    print("   Status: Enabled")
    print(f"   Node: {pnode}")


# Monitoring JFP Trainer setup/teardown/status  functions
async def run_trainer_op(parser):
    op = parser.trainer_op
    node = parser.node
    dur = getattr(parser, "dur", None)  # may be absent for status/teardown

    if op == "setup":
        logger.info("setting up JFP trainer...\n")

        if not check_ping("localhost", node):
            print(f"Can't setup JFP trainer. Node {node} is not reachable !")
            return

        if dur is None or dur <= 0:
            raise Exception("--dur must be a positive window (days); got {dur!r}")

        # Verify that monitoring dependencies (Kafka/OpenSearch) are operational       
        await JFP_UTIL.monitoring_services_check(check_running=True, check_enabled=True)

        # Validate Slurm REST API configuration settings
        JFP_UTIL.slurm_rest_api_setup_check(node)

        # Validate configuration of Slurm REST API data collector setup        
        JFP_UTIL.slurm_rest_api_collector_check(node)

        # Initialize JFP host environment
        await JFP_UTIL.initial_jfp_setup(node)

        # Initialize image if not already present (idempotent)
        jfp_image_tar = JFP_UTIL.get_jfp_image_tar(node)
        jfp_image_name = JFP_UTIL.get_jfp_image_name(node)
        JFP_UTIL.load_image(jfp_image_tar, jfp_image_name, node)

        # Detect current setup state
        trainer_enabled_flag = await monitoring_manifest.get_jfp_trainer_enabled()
        cron_exists = _cron_present(node)

        # Already configured? say so and keep services fresh
        if trainer_enabled_flag and cron_exists:
            update_trainer_status(node, dur)  # keep status yaml in sync
            print(f"JFP trainer already set up for {dur} days (cron present).")
            _mlflow_ensure_started(node)  # ensure mlflow stays up
            _restart_cron_service(node)  # harmless; ensures pickup
            return

        # Preserve existing active trainers from cron (if any)
        active_trainers = []
        if cron_exists:
            active_trainers = get_active_trainers(trainer_cron=trainer_cron, node=node)

        # Update cron to run 'all' trainers with provided duration
        update_trainer_cron(
            trainer_cron=trainer_cron,
            active_trainers=active_trainers,
            new_trainer="all",
            num_days=dur,
            node=node
        )

        # Write status stamp for status() readout
        update_trainer_status(node, dur)

        # Start mlflow (needed by trainer/logging) and restart cron
        _mlflow_ensure_started(node)
        _restart_cron_service(node)

        print(f"Successfully scheduled JFP trainer cron job for {dur} days")

        # Persist flags       
        await monitoring_manifest.set_jfp_trainer_enabled(val=True)

    elif op == "teardown":
        logger.info(f"tearing down jfp trainer...\n")

        node = await monitoring_manifest.get_host_with_jfp_setup()
        trainer_enabled_flag = await monitoring_manifest.get_jfp_trainer_enabled()
                
 
        # Nothing to do?
        if not trainer_enabled_flag and not _cron_present(node):
            print(f"[JFP trainer teardown] JFP trainer is not setup. It can be setup with 'cm monitoring jfp setup trainer --dur=7'.")
            return

        # If cron exists, remove it
        if _cron_present(node):
            remote_delete(node, trainer_cron)
            print(f"[JFP trainer teardown] Removed JFP trainer cron job")
        else:
            print(f"[JFP trainer teardown] No trainer cron file found; continuing teardown")

        # Update status/configs to defaults
        update_trainer_status(node)  # will write disabled/empty run window
        update_trainer_config_files(train_dur=None, node=node)

        # Restart cron to pick up removal
        _restart_cron_service(node)
                       
        # Clear trainer flag     
        await monitoring_manifest.set_jfp_trainer_enabled(val=False)

    elif op == "status":
        node = await monitoring_manifest.get_host_with_jfp_setup()
        trainer_enabled_flag = await monitoring_manifest.get_jfp_trainer_enabled()

        
        if not trainer_enabled_flag:
            print(f"JFP trainer is not setup, Status: Disabled.It can be setup with 'cm monitoring jfp setup trainer --dur=7'. ")
            return

        # Enabled but cron missing ? degraded
        if not _cron_present(node):
            print("   Status: Enabled (cron file missing: degraded)\n")
            return

        _print_enabled_status_header(node)

        # Load status yaml defensively
        try:
            status_cfg = load_yaml_file(status_yaml, node) or {}
        except Exception:
            status_cfg = {}

        dur_txt = None
        next_run = None
        last_run = None
        try:
            dur_txt = status_cfg.get("enable", {}).get("dur")
            run_sec = status_cfg.get("run", {})
            next_run = run_sec.get("next_scheduled_date")
            last_run = run_sec.get("run_date")
        except Exception:
            pass

        if dur_txt:
            print(f"   Train duration: {dur_txt} days")
        if next_run:
            print(f"   Next scheduled run: {next_run}")
        if last_run:
            print(f"   Last run: {last_run}")
        if not (dur_txt or next_run or last_run):
            print("   note: trainer status yaml missing fields; it will populate after first run")

    else:
        raise ValueError(f"Unknown trainer op: {op}")


########################################################################################
# JFP Slurm Rest collector helpers function

# Start Slurm Rest collector Services
def start_slurmrestcol_services(node):
    if not JFP_UTIL.is_slurm_rest_col_services_running(node):
        JFP_UTIL.enable_slurm_rest_col_services(node)
        JFP_UTIL.start_slurm_rest_col_services(node)

    # stop Slurm Rest collector Services


def stop_slurmrestcol_services(node):
    JFP_UTIL.stop_slurm_rest_col_services(node)
    JFP_UTIL.disable_slurm_rest_col_services(node)


##staus Slurm Rest collector Services 
def show_slurmrestcol_status(node, label):
    services = getattr(JFP_UTIL, "jfp_slurm_collector", [])
    if not services:
        print(f"Node: {node}\n   Status: Unknown (no collector services defined).\n")
        return False

    enabled_all = JFP_UTIL.is_slurm_rest_col_services_enabled(node)
    running_all = JFP_UTIL.is_slurm_rest_col_services_running(node)

    if enabled_all and running_all:
        print(f"Node: {label}\n   Status: Running (enabled)\n"
              f"   Services: {', '.join(services)}\n")
        return True

    disabled = [s for s in services if not JFP_UTIL._is_enabled(node, s)]
    inactive = [s for s in services if not JFP_UTIL._is_active(node, s)]

    print(f"Node: {label}\n   Status: Not healthy")
    if set(disabled) == set(inactive) and disabled:
        print(f"   Disabled & Inactive: {', '.join(disabled)}\n")
    else:
        if disabled:
            print(f"   Disabled: {', '.join(disabled)}")
        if inactive:
            print(f"   Inactive: {', '.join(inactive)}")
        print()
    return False


################
# Monitoring JFP Slurm Rest collector setup/teardown/status   
async def run_slurmrest_col_op(parser):
    op = parser.slurmrest_op
    node = parser.node

    label = "admin" if node == "localhost" else node
    intv = getattr(parser, "int", None)  # may be absent for status/teardown
    dur = getattr(parser, "wnd", None)  # may be absent for status/teardown

    if op == "setup":
        logger.info("setting up slurm rest collector: preparing data for JFP inference and training...\n")

        if not check_ping("localhost", node):
            print(f"Can't setup slurm rest collector. Node {node} is not reachable !")
            return

        if not (isinstance(intv, (int, float)) and intv > 0):
            raise ValueError(f"--int must be a positive interval (seconds) for slurmrest[runtime]; got {intv!r}")

        if not (isinstance(dur, (int, float)) and dur > 0):
            raise ValueError(f"--wnd must be a positive window (days) for slurmdb[history]; got {dur!r}")

        # Verify that monitoring dependencies (Kafka/OpenSearch) are operational   
        await JFP_UTIL.monitoring_services_check(check_running=True, check_enabled=True)

        # Initialize slurm rest collector host environment config setup 
        await JFP_UTIL.setup_initial_slurm_config(node, parser)
         
        # Validate Slurm REST API configuration settings
        JFP_UTIL.slurm_rest_api_setup_check(node)

        # Validate configuration of Slurm REST API response/request setup   
        JFP_SLURM_UTIL.slurm_rest_api_status_check(node)

        # Detect current setup state
        slurmrest_enabled_flag = await monitoring_manifest.get_jfp_slurmrest_enabled()

        # Already configured? say so and keep services fresh
        if slurmrest_enabled_flag:
            print(
                f"[JFP slurm rest collectors setup] slurm rest collectors already configured: slurmrest[runtime] {intv} sec, slurmdb[history] {dur} day.\n")
            start_slurmrestcol_services(node)  # ensure slurm rest
            return

        # if not setup Start the  slurm rest collector service 
        start_slurmrestcol_services(node)
        
        if not JFP_UTIL.is_slurm_rest_col_services_running(node):
            print("Slurm REST collector service not started. check the logs for detail.")
        else:        
            print(f"Successfully scheduled Slurm REST collector: slurmrest[runtime] {intv} sec, slurmdb[history] {dur} day.\n")  
            # set the Persist flags in monitoring_manifest        
            await monitoring_manifest.set_jfp_slurmrest_enabled(val=True)

    elif op == "teardown":
        logger.info("tearing down slurm rest collector...\n")

        node = await monitoring_manifest.get_host_with_jfp_setup()
        slurmrest_enabled_flag = await monitoring_manifest.get_jfp_slurmrest_enabled()
        
        # check  slurmrest_enabled_flag 
        if not slurmrest_enabled_flag:
            print(f"[JFP slurm rest collectors teardown] Slurm rest collector is not setup. It can be setup with 'cm monitoring jfp setup slurmrest-col --h'.\n")
            return
        
         #call trainer teardown and slumrest collector teardown if enabled    
           
        if node:      
            print(f"[JFP slurm rest collectors teardown] JFP should be teardown using command:'cm monitoring jfp teardown' ")      
            return              
        
        setup_yn= await monitoring_manifest.get_jfp_trainer_enabled()   
        
        if setup_yn:
            print(f"[JFP slurm rest collectors teardown] JFP trainer should be teardown using command:'cm monitoring jfp teardown trainer'") 
            return
        
        stop_slurmrestcol_services(node)                                        

        # clear the slumrest flag from the monitoring_manifest        
        await monitoring_manifest.set_jfp_slurmrest_enabled(val=False)

    elif op == "status":
        node = await monitoring_manifest.get_host_with_jfp_setup()
        slurmrest_enabled_flag = await monitoring_manifest.get_jfp_slurmrest_enabled()
        
        if not slurmrest_enabled_flag:
             print(f"JFP Slurm rest collector is not setup. It can be setup with 'cm monitoring jfp setup slurmrest-col --h'.\n")
        show_slurmrestcol_status(node, label)
        
    else:
        raise ValueError(f"Unknown trainer op: {op}")


##########################################################################################################################
# JFP service management option function /setup/trainer/slurmrest-col/restart

async def run_async(args: argparse.Namespace):
    monitoring_logging.setup_logging(args)

    # TODO: Should rebalance be under config?
    # What about delete and recreate all topics
    # What about rerun the topic create to pick up new topics
    # Ditto for schemas
    try:
        assert platform.machine() != 'arch64', 'arch64 not supported'

        zk_enabled = await monitoring_manifest.get_zookeeper_enabled()
        if not zk_enabled and (args.parent_op == 'setup' or args.parent_op == 'teardown') and not (
                hasattr(args, 'force') and args.force):
            print(
                "Can not perform setup or teardown command. Zookeeper must be setup first. For teardown use --force to bypass.")
            return

        if args.parent_op == 'setup':
            if not args.type:
                await setup(args.node)
            elif args.type == 'trainer':
                await run_trainer_op(args)
            elif args.type == 'slurmrest-col':
                await run_slurmrest_col_op(args)
        elif args.parent_op == 'teardown':
            if not args.type:
                await teardown()
            elif args.type == 'trainer':
                await run_trainer_op(args)
            elif args.type == 'slurmrest-col':
                await run_slurmrest_col_op(args)
        elif args.parent_op == 'status':
            if not args.type:
                print(await status())
            elif args.type == 'trainer':
                await run_trainer_op(args)
            elif args.type == 'slurmrest-col':
                await run_slurmrest_col_op(args)
        elif args.parent_op == 'restart':
            await run_restart_op()
        else:
            raise Exception(f'Unknown operation: {args.parent_op}')
    except Exception as e:
        monitoring_logging.log_and_print_exception_and_exit(e, logger, getattr(args, "verbose", False))

 

##########################################################################################################################
# run function

def run(args: argparse.Namespace):
    asyncio.run(run_async(args))


def parse_args(parser: argparse.ArgumentParser):
    op = parser.add_subparsers(dest='parent_op', required=True)

    # Setup
    setup_op = op.add_parser('setup',
                             help='Setup JFP  on the specified nodes; the default is the management node.')
    setup_op.formatter_class = CustomHelpFormatter
    add_verbose_output_arg(setup_op)
    setup_op.add_argument('type', nargs='?', choices=['trainer', 'slurmrest-col'],
                          help='Type of JFP component to setup')
    setup_op.add_argument('-n', '--node', dest='node', action='store', default='localhost',
                          help='the node to setup JFP on')

    setup_op.add_argument('--dur', dest='dur', metavar='DAYS', default=7,
                          type=int, action='store',
                          help='Training window for the JFP trainer, in days;Default=%(default)s, '
                               'the number of past days of job history to include when training the model(for trainer)')

    setup_op.add_argument('--rest-host', dest='rest_host', metavar='HOST',
                          type=str, action='store', default='localhost',
                          help='Hostname or IP where slurmrestd is running (e.g., localhost);'
                               'Default %(default)s(for slurmrest-col)')

    setup_op.add_argument('--rest-port', dest='rest_port', metavar='PORT',
                          type=int, action='store', default=6820,
                          help='TCP port for slurmrestd (/slurm REST, plugin openapi/slurm);'
                               'Default %(default)s(for slurmrest-col)')

    # todo change defualt=6820
    setup_op.add_argument('--restdb-port', dest='restdb_port', metavar='PORT',
                          type=int, action='store', default=6821,
                          help='TCP port for slurmrestd (/slurmdb REST, plugin openapi/slurm).'
                               'Example=6821?;Default %(default)s(for slurmrest-col)')

    setup_op.add_argument('--install-path', dest='install_path', metavar='PATH',
                          type=str, action='store', default='/opt/slurm/bin',
                          help='Path to Slurm binaries (e.g., /opt/slurm/bin);Default %(default)s(for slurmrest-col)')

    setup_op.add_argument('--jwt-user', dest='jwt_user', metavar='USER',
                          type=str, action='store', default='jwtuser',
                          help='Default username used to mint Slurm REST JWTs.'
                               'Example jwtuser;Default %(default)s(for slurmrest-col)')

    setup_op.add_argument('--jwt-lifespan', dest='jwt_lifespan', metavar='SECS',
                          type=int, action='store', default=900,
                          help='JWT lifetime in seconds for Slurm REST tokens.'
                               'Example 900;Default %(default)s(for slurmrest-col)')

    setup_op.add_argument('--log-level', dest='log_level', metavar='LEVEL',
                          type=str.upper, choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                          action='store', default='INFO',
                          help='Log verbosity for the collector (CRITICAL|ERROR|WARNING|INFO|DEBUG)'
                               'Example INFO or DEBUG;Default %(default)s(for slurmrest-col)')

    setup_op.add_argument('--int', dest='int', metavar='SECS',
                          type=int, action='store', default=45,
                          help='Polling interval for the Slurm REST runtime collector (/slurm), in seconds '
                               'Recommended 30-60;Default=%(default)s(for slurmrest-col)')

    setup_op.add_argument('--wnd', dest='wnd', metavar='DAYS',
                          type=int, action='store', default=1,
                          help='Polling window for the SlurmDB history collector (/slurmdb), in days '
                               'Recommended 1-2;Default=%(default)s(for slurmrest-col)')

    # For compatibility with run_*_op functions
    setup_op.set_defaults(trainer_op='setup', slurmrest_op='setup')

    # Teardown
    teardown_op = op.add_parser('teardown',
                                help='Teardown JFP from the specified nodes; the default is all nodes currently running jfp.')
    teardown_op.formatter_class = CustomHelpFormatter
    teardown_op.add_argument('-n', '--node', dest='node', action='store', default='localhost',
                             help='the node to setup JFP on')
    add_verbose_output_arg(teardown_op)
    teardown_op.add_argument('type', nargs='?', choices=['trainer', 'slurmrest-col'],
                             help='Type of JFP component to teardown')
    # For compatibility with run_*_op functions
    teardown_op.set_defaults(trainer_op='teardown', slurmrest_op='teardown')

    # Status
    status_op = op.add_parser('status', help='Display status of JFP components')
    status_op.formatter_class = CustomHelpFormatter
    add_verbose_output_arg(status_op)
    status_op.add_argument('-n', '--node', dest='node', action='store', default='localhost',
                           help='the node to setup JFP on')
    status_op.add_argument('type', nargs='?', choices=['trainer', 'slurmrest-col'],
                           help='Type of JFP component to check status')
    # For compatibility with run_*_op functions
    status_op.set_defaults(trainer_op='status', slurmrest_op='status')

    restart_op = op.add_parser('restart', help='Restart JFP  services')
    add_verbose_output_arg(restart_op)


##########################################################################################################################  


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Advanced management functions for Job Failure Prediction  monitoring")
    parse_args(p)
    _args = p.parse_args()
    run(_args)
