# Cloud: S3, Snowflake, Databricks

> **Status: NOT RUN, NEEDS ACCOUNTS.** Everything here is written to run,
> but no AWS, Snowflake or Databricks account was created or used, no
> credentials were entered and no money was spent. The local DuckDB path
> stays the default. What *was* verified without accounts is listed under
> each part.

```
data/raw/*.xlsx ──upload──> S3  raw/robot_logs/ingest_date=YYYY-MM-DD/*.xlsx      (original, unchanged)
                            S3  staged/robot_logs/ingest_date=YYYY-MM-DD/*.csv    (same rows, CSV)
                                   │                               │
                       COPY INTO (02_load_raw.sql)         Databricks job (PySpark)
                                   ▼                               ▼
                         Snowflake RAW.RAW_LOGS         Delta table stg_robot_logs_spark
                                   │                     (+ success test, fails the job)
                    dbt build --target snowflake
                                   ▼
                 Snowflake ANALYTICS.stg_robot_logs, fct_stop_events, agg_* (+ all dbt tests)
```

Install the extras into the project venv:

```bash
pip install -r requirements-cloud.txt
```

---

## 1. Raw files in AWS S3

**Code:** `robot_pipeline/cloud/s3.py`. It validates each file with the
loader's rules, uploads the original `.xlsx` byte-for-byte, and writes a
CSV copy with the same values (Snowflake cannot read Excel). Re-uploading
identical content is skipped: the SHA-256 is stored as object metadata.

**Verified without an account:** 7 tests against `moto` (an in-memory
fake of S3), also run in CI: keys and layout, original bytes unchanged,
the CSV keeps every value (stray-quoted timestamps and NULL stops
included), idempotent re-upload, invalid file never uploaded, download
returns identical bytes. The CSV path was also run on the real file and
fed to Spark (section 3).

**Steps (needs an AWS account; S3 storage for a 2 MB file costs very little, but check current pricing):**

1. Create a private bucket with versioning on and public access blocked.
2. Create an IAM role or user for the pipeline with
   [`aws/s3-pipeline-policy.json`](aws/s3-pipeline-policy.json) (only
   the two prefixes, only get/put/list). Replace `<your-bucket>`.
3. Configure credentials **outside the repo**, for example
   `aws configure sso` and then `export AWS_PROFILE=<profile>`. Never put
   keys in `.env` files that could be committed.
4. Upload:

   ```bash
   export ROBOT_S3_BUCKET=<your-bucket>
   python -m robot_pipeline.cloud.s3 upload
   ```

5. Optional, S3 as the local pipeline's source:
   `python -m robot_pipeline.cloud.s3 download --date 2026-11-26`, then
   `python ingest.py`.

---

## 2. Snowflake as the warehouse (second dbt target)

**Code:** the `snowflake` output in [`../dbt/profiles.yml`](../dbt/profiles.yml)
(key-pair auth, every value from an environment variable),
[`snowflake/01_setup.sql`](snowflake/01_setup.sql) (warehouse, database,
schemas, role, service user, raw table with the exact source column
names, S3 storage integration, file format, stage) and
[`snowflake/02_load_raw.sql`](snowflake/02_load_raw.sql) (COPY into a
temporary table, then delete and insert per `source_file` in one
transaction, so reloads never duplicate; the load aborts if any row
arrives without its file name). The same dbt models run on
both targets. The only engine-specific SQL is the timestamp parse, in
`dbt/macros/parse_iso_utc_ts.sql` (an explicit format on Snowflake).

**Verified without an account:**
- `DBT_TARGET=snowflake dbt parse` succeeds: the profile, project and
  adapter (dbt-snowflake 1.12.5) load.
- All 37 compiled models and tests parse as Snowflake SQL with `sqlglot`.
  That is a syntax check of the SQL as written; the Snowflake versions of
  the dbt macros could not be rendered without a connection.
- **Not verified:** `dbt compile` and `dbt build` on Snowflake (they need
  a connection), and the setup and load SQL, which was written from
  Snowflake's documentation and not executed.

**Steps (needs a Snowflake account; the XSMALL warehouse auto-suspends after 60 s):**

1. Make a key pair for the service user (keep the private key outside the repo):

   ```bash
   openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM -out ~/.snowflake/rsa_key.p8
   openssl rsa -in ~/.snowflake/rsa_key.p8 -pubout -out ~/.snowflake/rsa_key.pub
   ```

2. In a Snowflake worksheet, fill in the `<...>` placeholders and run
   `snowflake/01_setup.sql`.
3. **S3 → Snowflake:** in AWS, create the role named in the storage
   integration with [`aws/snowflake-read-policy.json`](aws/snowflake-read-policy.json).
   Run `DESC INTEGRATION ROBOT_S3_INT;` and put `STORAGE_AWS_IAM_USER_ARN`
   and `STORAGE_AWS_EXTERNAL_ID` into that role's trust policy.
4. Load a day: replace `<YYYY-MM-DD>` in `snowflake/02_load_raw.sql` and run it.
5. Run dbt against Snowflake:

   ```bash
   export DBT_TARGET=snowflake ROBOT_RAW_SCHEMA=RAW
   export SNOWFLAKE_ACCOUNT=<org-account> SNOWFLAKE_USER=ROBOT_DBT
   export SNOWFLAKE_PRIVATE_KEY_PATH=~/.snowflake/rsa_key.p8
   export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=<passphrase>   # or omit if the key is unencrypted
   python -m robot_pipeline.dbt_runner build
   ```

   The success test runs as dbt tests on Snowflake. (`python -m
   robot_pipeline.checks` and the monitor read DuckDB only.)

---

## 3. One transformation in Databricks

**Code:** [`databricks/src/clean_robot_logs.py`](databricks/src/clean_robot_logs.py)
does the cleaning of `stg_robot_logs` in PySpark and runs the success test
before writing. If it fails, the job fails and nothing is written. If
the same file was uploaded on several days, only its latest
`ingest_date` copy is used. The
bundle is [`databricks/databricks.yml`](databricks/databricks.yml) plus
[`databricks/resources/clean_robot_logs.job.yml`](databricks/resources/clean_robot_logs.job.yml)
(one serverless Python task). Databricks now calls bundles "Declarative
Automation Bundles" (formerly Asset Bundles).

**Verified without an account:** the same script ran on **Apache Spark
4.3.0 locally** on the real file (via the staged CSV): success test
passed (7,409 rows; 278 stops; 54 unknown; 0 invalid; 0 unparsed
timestamps). Its output was **identical to dbt's `stg_robot_logs`** on
all 30 shared columns for all 7,409 rows (`EXCEPT` both ways: 0 rows).
Two Spark tests on synthetic data run in CI.
**Not verified:** running on Databricks itself, the Delta write and
Unity Catalog permissions.

**Steps (needs a Databricks workspace; Databricks offers a no-cost Free
Edition with limits, check its current terms):**

1. Install the Databricks CLI (not the old `databricks-cli` pip package):
   `brew tap databricks/tap && brew install databricks`.
2. `databricks auth login --host https://<your-workspace>.cloud.databricks.com`
   (browser login; no token in files).
3. Set `workspace.host` in `databricks.yml`, and the `staged_input` and
   `output_table` variables. The workspace needs read access to the S3
   prefix (an external location / storage credential in Unity Catalog),
   or upload the CSVs to a Unity Catalog volume and point `staged_input`
   at it.
4. From `cloud/databricks/`:

   ```bash
   databricks bundle validate
   databricks bundle deploy --target dev
   databricks bundle run clean_robot_logs
   ```

To run the same script locally (needs Java 17+): `pip install
pyspark==4.3.0`, then `python cloud/databricks/src/clean_robot_logs.py
--input <folder of staged CSVs> --output <out folder> --format parquet`.
