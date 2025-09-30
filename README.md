# Data Ingestion Scheduler

This project automates the execution of data ingestion scripts, validates their output, and manages Solr insert/update
operations with backup and logging.

## How It Works

- Reads a schedule from `config/ingestion_schedule.json`.
- For each enabled script, checks if it should run (based on interval or force).
- Executes the script, passing parameters for output and logging.
- Validates generated JSON files for Solr insert/update.
- Backs up Solr documents before updates.
- Optionally commits changes to Solr.
- Logs all actions and appends run history to `data/run_history.json`.

## Schedule File Format

The `config/ingestion_schedule.json` file should look like:

```json
{
  "settings": {
    "commit_solr": true
  },
  "scripts": {
    "script1": {
      "script_file": "example_script.py",
      "last_run": "2024-06-01",
      "interval_days": 2,
      "force_run": false,
      "disabled": false,
      "solr_insert": [
        {
          "core_name": "example_core",
          "key": "id"
        }
      ],
      "solr_update": [
        {
          "core_name": "example_core",
          "key": "id",
          "fields": [
            "field1",
            "field2"
          ]
        }
      ]
    }
  }
}
```

- `settings.commit_solr`: If true, commits changes to Solr after validation. Default is `false`.
- Each script entry:
    - `script_file`: Path to the script to execute (relative to `scripts/`).
    - `last_run`: Last run date in `YYYY-MM-DD` format.
    - `interval_days`: Days between runs.
    - `force_run`: If true, runs regardless of schedule. Default is `false`.
    - `disabled`: If true, script is skipped. Default is `true`.
    - `solr_insert`/`solr_update`: List of Solr core configs for insert/update, with required `core_name`, `key`, and
      for updates, allowed `fields`.

## Usage

1. Place your ingestion scripts in the `scripts/` directory.
2. Edit `config/ingestion_schedule.json` to schedule and configure scripts.
3. Run the scheduler:

   ```sh
   python run_data_ingestion.py
   ```

4. Logs are written to `logs/`, and run history to `data/run_history.json`.

## Script Arguments

Each ingestion script **must** accept the following command-line arguments:

- `--date`: The date (in `YYYY-MM-DD` format) to fetch data from.
- `--work_dir`: Directory where the script should generate its output files and logs.
- `--insert_file`: (Optional) Path to write the Solr insert JSON file, if applicable.
- `--update_file`: (Optional) Path to write the Solr update JSON file, if applicable.

The scheduler will provide these arguments when running each script. Scripts should generate the specified output files
if the corresponding argument is given. The scheduler will validate the output files after script execution.

**Example Script Output for Insert (JSON):**

```json
[
  {
    "id": "123",
    "field1": "value1",
    "field2": "value2"
  },
  {
    "id": "124",
    "field1": "value3",
    "field2": "value4"
  }
]
```

**Example Script Output for Update (JSON):**

```json
[
  {
    "id": "123",
    "field1": {
      "set": "new_value1"
    },
    "field2": {
      "set": "new_value2"
    }
  },
  {
    "id": "124",
    "field1": {
      "set": "new_value3"
    }
  }
]

```

- Each field to update uses `{ "set": value }` to indicate an atomic update.
- The `id` field identifies the document to update.
- Fields not included in the object will remain unchanged in Solr.

## Requirements

- Python 3.7+
- `requests` library
- Access to `Solr` and the `p3-solr-insert` command-line tool

## Testing

Unit tests are provided in `test_run_data_ingestion.py`. To run the tests:

```sh
python -m unittest test_run_data_ingestion.py
```

## Directory Structure

- `run_data_ingestion.py` — Main scheduler script
- `config/ingestion_schedule.json` — Schedule and configuration file
- `scripts/` — Your ingestion scripts
- `logs/ — Log files`
- `data/run_history.json` — Run history log