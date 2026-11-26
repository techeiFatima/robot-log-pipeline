# Airflow: daily schedule

`dags/robot_log_pipeline.py` defines one workflow, `robot_log_pipeline`,
that runs every day at midnight UTC:

```
load_raw_logs  ->  dbt_build  ->  success_check
```

- **Retries:** each step retries twice, 5 minutes apart then longer
  (exponential backoff), and is stopped after 30 minutes.
- **Failure states:** a failed step turns red; the steps after it become
  `upstream_failed` and do not run. So the first red box is the cause.
  When a step fails for good, `on_failure_callback` logs (and from
  Phase 8 also sends) an alert naming the task, the attempts and the run.
- **Isolation:** Airflow runs in its own virtualenv. Each step calls the
  project's `.venv` Python, so Airflow's many dependencies never clash
  with the pipeline's pinned versions.

## Run it on your laptop (macOS, no Docker needed)

Airflow 3.4.0 supports Python 3.11–3.14. From the repository root, with
the project's `.venv` already set up (see the main README):

```bash
python3.13 -m venv ~/.venvs/airflow
```

```bash
~/.venvs/airflow/bin/pip install "apache-airflow==3.4.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.13.txt"
```

```bash
export AIRFLOW_HOME="$PWD/orchestration/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/orchestration/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export ROBOT_PROJECT_DIR="$PWD"
export ROBOT_PYTHON="$PWD/.venv/bin/python"
```

Run the whole workflow once, without the scheduler (good for checking):

```bash
~/.venvs/airflow/bin/airflow db migrate
~/.venvs/airflow/bin/airflow dags test robot_log_pipeline
```

Or start the full local Airflow (web UI on http://localhost:8080; the
admin password is printed in the terminal and saved in
`orchestration/airflow_home/`):

```bash
~/.venvs/airflow/bin/airflow standalone
```

After every run, `state/pipeline_run_latest.json` records the run's final
status (`success` or `failed`, plus the failed task). Read it instead of
the check report alone: when an early step fails, the success check never
runs, so its last report would be out of date.

`airflow standalone` is for development only. In production you would use
the official Docker Compose file or a managed service (for example Amazon
MWAA), with the same DAG file.

### Deploying elsewhere (Docker, Amazon MWAA)

The steps call `ROBOT_PYTHON` inside `ROBOT_PROJECT_DIR`, so the Airflow
workers must be able to see both. In Docker, mount the repository and
build its `.venv` inside the image (or set `ROBOT_PYTHON` to a Python that
has `requirements.txt` installed). On a managed service, package the
project with its dependencies into the worker environment and point the
two variables at it. If they are wrong, every step fails with "No such
file or directory".

`orchestration/airflow_home/` is ignored by Git.
