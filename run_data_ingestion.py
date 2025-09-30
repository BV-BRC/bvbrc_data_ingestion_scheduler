#!/usr/bin/env python

import datetime
import json
import logging
import requests
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

MAIN_DIR = Path(".")
CONFIG_DIR = MAIN_DIR / "config"
SCRIPTS_DIR = MAIN_DIR / "scripts"
BACKUP_DIR = MAIN_DIR / "backup"
LOG_DIR = MAIN_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

run_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = LOG_DIR / f"{run_timestamp}_data_ingestion.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Log to the console as well
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(console_handler)

DATA_DIR = MAIN_DIR / "data"
OUTPUT_BASE_DIR = DATA_DIR / "output"
RUN_HISTORY_FILE = DATA_DIR / "run_history.json"
SCHEDULE_FILE = CONFIG_DIR / "ingestion_schedule.json"

BVBRC_API_URL = "https://www.bv-brc.org/api"


def append_run_history(entry: Dict[str, Any]) -> None:
    """Append a run entry to the run history file."""
    history = []
    if RUN_HISTORY_FILE.exists():
        try:
            history = json.loads(RUN_HISTORY_FILE.read_text())
        except json.JSONDecodeError:
            logging.warning("Corrupt run history file. Overwriting.")

    history.append(entry)
    RUN_HISTORY_FILE.write_text(json.dumps(history, indent=2))


def load_schedule() -> Dict[str, Any]:
    """Load schedule metadata from the JSON file."""
    if not SCHEDULE_FILE.exists():
        logging.info(f"Schedule file not found: {SCHEDULE_FILE}")
        return {}

    try:
        logging.info(f"Reading schedule file: {SCHEDULE_FILE}")
        return json.loads(SCHEDULE_FILE.read_text())
    except json.JSONDecodeError as e:
        logging.error(f"Cannot parse schedule file: {e}")
        return {}


def save_schedule(schedule: Dict[str, Any]) -> None:
    """Save the schedule metadata to the JSON file."""
    SCHEDULE_FILE.write_text(json.dumps(schedule, indent=2))


def get_next_run_date(script_meta: Dict[str, Any]) -> datetime.datetime:
    """Calculate the next run date based on last run + interval_days."""
    last_run_date = datetime.datetime.fromisoformat(script_meta["last_run"])
    return last_run_date + datetime.timedelta(days=script_meta["interval_days"])


def should_run(script_meta: Dict[str, Any]) -> Tuple[bool, str]:
    """Check if the script should run today or if force_run is enabled."""
    if script_meta.get("force_run", False):
        return True, "Forced execution"

    next_run_date = get_next_run_date(script_meta)
    return datetime.datetime.now() >= next_run_date, next_run_date.isoformat()


def update_run_date(script_name: str, schedule: Dict[str, Any], timestamp: str) -> None:
    """Update the last run date after execution and reset force_run."""
    schedule["scripts"][script_name]["last_run"] = timestamp
    # schedule[script_name]["force_run"] = False
    save_schedule(schedule)


def create_output_dir(run_date: str, script_name: str) -> Path:
    """Create a base output directory for the given script."""
    output_dir = OUTPUT_BASE_DIR / run_date / script_name
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def create_backup_dir(run_date: str, script_name: str) -> Path:
    """Create a base backup directory for the given script."""
    output_dir = BACKUP_DIR / run_date / script_name
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def execute_script(script_name: str, script_file: str, last_run_date: str,
                   has_insert: bool, has_update: bool) -> Tuple[Optional[Path], Optional[Path]]:
    """Run the script for the given date and return insert/update file paths if generated."""
    logging.info(f"Executing {script_name} ({script_file})...")

    script_path = SCRIPTS_DIR / script_file
    if not script_path.is_file():
        raise FileNotFoundError(f"Script file not found: {script_path}")

    # Determine the current date for the output folder
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    output_dir = create_output_dir(today, script_name)

    output_insert_file = output_dir / f"{script_name}_insert.json" if has_insert else None
    output_update_file = output_dir / f"{script_name}_update.json" if has_update else None
    log_file_path = output_dir / f"{script_name}.log"

    command = [
        str(script_path),
        "--date", last_run_date,
        "--work_dir", str(output_dir)
    ]
    if has_insert:
        logging.info(f"{script_name} will generate insert file: {output_insert_file}")
        command.extend(["--insert_file", str(output_insert_file)])
    if has_update:
        logging.info(f"{script_name} will generate update file: {output_update_file}")
        command.extend(["--update_file", str(output_update_file)])

    logging.info(f"Running command: {' '.join(command)} (logging to {log_file_path})")

    try:
        with log_file_path.open("w") as log_file:
            subprocess.run(
                command,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=True
            )
    except subprocess.CalledProcessError as e:
        logging.info(f"{script_name} failed — see log: {log_file_path}")
        for f in [output_insert_file, output_update_file]:
            if f and f.exists():
                try:
                    f.unlink()
                    logging.info(f"Removed incomplete file: {f}")
                except Exception as cleanup_err:
                    logging.warning(f"Could not remove {f}: {cleanup_err}")
        raise RuntimeError(f"Execution of {script_name} failed") from e

    logging.info(f"{script_name} finished successfully — log: {log_file_path}")
    return output_insert_file, output_update_file


def validate_json(file_path: Path) -> List[Dict[str, Any]]:
    """Check JSON syntax and ensure it's a list of docs."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("Solr expects a JSON array of documents/updates.")
        return data
    except Exception as e:
        raise ValueError(f"Invalid JSON in {file_path}: {e}")


def validate_solr_insert_file(file_path: Path, unique_key: str = "id") -> None:
    """Validate insert file has proper JSON docs with unique key."""
    docs = validate_json(file_path)
    for doc in docs:
        if not isinstance(doc, dict):
            raise ValueError(f"Each document must be a JSON object, got: {doc}")

        if unique_key not in doc:
            raise ValueError(f"Missing unique key '{unique_key}' in doc: {doc}")

        # disallow accidental update operators
        for field, value in doc.items():
            if isinstance(value, dict) and any(op in value for op in ["set", "add", "remove", "inc"]):
                raise ValueError(
                    f"Invalid insert value for field '{field}' in {file_path}: {value}"
                )

    logging.info(f"{file_path} passed insert validation ({len(docs)} docs).")


def validate_solr_update_file(file_path: Path, allowed_fields: List[str], unique_key: str) -> List[Dict[str, Any]]:
    """Validate update file matches allowed fields and syntax."""
    docs = validate_json(file_path)
    for doc in docs:
        if not isinstance(doc, dict):
            raise ValueError(f"Each document must be an object, got: {doc}")

        # Must include a unique key
        if unique_key not in doc:
            raise ValueError(f"Missing unique key '{unique_key}' in doc: {doc}")

        for field, value in doc.items():
            if field == unique_key:
                continue
            if field not in allowed_fields:
                raise ValueError(
                    f"Unexpected field '{field}' in {file_path}. "
                    f"Allowed fields: {allowed_fields}"
                )
            if not isinstance(value, dict) or not any(op in value for op in ["set", "add", "remove", "inc"]):
                raise ValueError(
                    f"Invalid update format for '{field}' in {file_path}: {value}"
                )
    logging.info(f"{file_path} validated with {len(docs)} docs and fields {allowed_fields}")
    return docs


def backup_solr_docs(backup_file: Path, core_name: str, unique_key: str, ids: List[str]) -> None:
    """Backup Solr docs for given IDs before update using POST"""
    # Build q parameter like: genome_id:(id1 OR id2 OR id3)
    q = f"{unique_key}:({' OR '.join(ids)})"

    headers = {
        "Accept": "application/solr+json",
        "Content-Type": "application/solrquery+x-www-form-urlencoded"
    }

    data = {
        "q": q,
        "wt": "json",
        "rows": len(ids)  # make sure we fetch all
    }

    url = f"{BVBRC_API_URL}/{core_name}"
    resp = requests.post(url, headers=headers, data=data)
    resp.raise_for_status()
    docs = resp.json().get("response", {}).get("docs", [])

    # Save backup
    with open(backup_file, "w") as f:
        json.dump(docs, f, indent=2)

    logging.info(f"Backed up {len(docs)} documents from {core_name} to {backup_file}")


def commit_solr_changes(core_name: str, data_file: Path, mode: str) -> None:
    """
    Commit Solr changes by running p3-solr-insert with --insert or --update.
    """
    if mode not in ["insert", "update"]:
        raise ValueError(f"Invalid mode: {mode}. Expected 'insert' or 'update'.")

    if not data_file.exists():
        raise FileNotFoundError(f"Data file not found: {data_file}")

    logging.info(f"Committing {mode} changes to SOLR for core: {core_name}")
    cmd = ["p3-solr-insert", f"--{mode}", core_name, str(data_file)]

    try:
        logging.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        if result.stdout.strip():
            logging.info(f"STDOUT:\n{result.stdout.strip()}")
        if result.stderr.strip():
            logging.info(f"STDERR:\n{result.stderr.strip()}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Running {' '.join(cmd)} (exit {e.returncode})")
        if e.stdout:
            logging.info(f"STDOUT:\n{e.stdout}")
        if e.stderr:
            logging.info(f"STDERR:\n{e.stderr}")
        raise

    logging.info(f"Successfully committed {mode} changes to SOLR")


def main() -> None:
    """Main function to process scheduled scripts."""
    schedule = load_schedule()
    if not schedule:
        logging.info("No schedule file found.")
        return

    settings = schedule.get("settings", {})
    scripts = schedule.get("scripts", {})
    if not scripts:
        logging.info("No scripts found in the schedule.")
        return

    for script_name, script_meta in scripts.items():
        logging.info(f"Processing {script_name} from the scheduler.")
        run_time = datetime.datetime.now()
        insert_file = None
        update_file = None
        backup_file = None
        status = "skipped"
        reason = "not scheduled"

        try:
            # Skip scripts that are disabled
            if script_meta.get("disabled", True):
                logging.info(f"{script_name} is disabled — skipping execution.")
                reason = "disabled"
                continue

            script_file = script_meta.get("script_file")
            if not script_file:
                logging.info(f"Skipping {script_name}: No script file specified.")
                reason = "no script_file"
                continue

            should_execute, reason = should_run(script_meta)
            if not should_execute:
                logging.info(f"{script_name} not due yet. Next run: {reason}")
                continue

            logging.info(f"{script_name} is running (Reason: {reason})")
            has_insert = bool(script_meta.get("solr_insert"))
            has_update = bool(script_meta.get("solr_update"))

            # Execute the script
            insert_file, update_file = execute_script(
                script_name, script_file, script_meta["last_run"], has_insert, has_update
            )

            if insert_file and insert_file.exists():
                logging.info(f"Validating insert file: {insert_file}")
                insert_conf = script_meta.get("solr_insert", [])[0]  # only one core assumed
                if "key" not in insert_conf:
                    raise KeyError(f"solr_insert config for {script_name} is missing 'key'")
                validate_solr_insert_file(insert_file, insert_conf["key"])

            if update_file and update_file.exists():
                logging.info(f"Validating update file: {update_file}")
                update_conf = script_meta.get("solr_update", [])[0]  # only one core assumed
                if "key" not in update_conf:
                    raise KeyError(f"solr_update config for {script_name} is missing 'key'")
                unique_key = update_conf["key"]
                allowed_fields = update_conf.get("fields", [])
                docs = validate_solr_update_file(update_file, allowed_fields, unique_key)

                core_name = update_conf["core_name"]
                logging.info(f"Processing backup before updating {core_name} core.")
                ids = [doc[unique_key] for doc in docs if unique_key in doc]

                today = run_time.strftime("%Y-%m-%d")
                backup_dir = create_backup_dir(today, script_name)
                backup_file = backup_dir / f"{script_name}_backup.json"
                backup_solr_docs(backup_file, core_name, unique_key, ids)

            if not (insert_file or update_file):
                logging.info(f"{script_name} didn't generate output files.")

            if settings.get("commit_solr", False):
                if insert_file and insert_file.exists():
                    insert_conf = script_meta.get("solr_insert", [])[0]
                    commit_solr_changes(insert_conf["core_name"], insert_file, "insert")

                if update_file and update_file.exists():
                    update_conf = script_meta.get("solr_update", [])[0]
                    commit_solr_changes(update_conf["core_name"], update_file, "update")
            else:
                logging.info("commit_solr = false. Skipping Solr commit.")

            update_run_date(script_name, schedule, run_time.strftime("%Y-%m-%d"))
            status = "success"

        except Exception as e:
            logging.error(f"[{script_name}] FAILED: {e}")
            status = "failure"
            reason = str(e)

        finally:
            # Always append history
            append_run_history({
                "script": script_name,
                "run_time": run_time.isoformat(),
                "status": status,
                "reason": reason,
                "insert_file": str(insert_file) if insert_file else None,
                "update_file": str(update_file) if update_file else None,
                "backup_file": str(backup_file) if backup_file else None,
            })


if __name__ == "__main__":
    main()

