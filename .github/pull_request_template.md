## What changed

## Why

## How it was checked
- [ ] `ruff check .` and `ruff format --check .`
- [ ] `pytest`
- [ ] `python -m robot_pipeline.dbt_runner build` (on the real file locally, or the SYNTHETIC sample)
- [ ] `python -m robot_pipeline.checks` passes

## Reviewer notes
<!-- Anything a reviewer should look at closely: data changes, new assumptions, risks. -->

- [ ] No data, database files or secrets are included
