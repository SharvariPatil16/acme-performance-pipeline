# Test evidence — `acme-performance` pipeline

All results below were produced on the workshop VDI (Ubuntu 22.04.5, Python 3.13.16,
OpenJDK 21, PySpark 4.1.2) against the local PostgreSQL source (`acme_performance`) and
LocalStack S3 (`coding-workshop-datalake-1bd9f4ac`), on 2026-10-14.

## 1. Automated test suite

```bash
./data/acme-performance/run_tests.sh -q
```

| Result | Value |
|---|---|
| Tests | **69 passed**, 0 failed (111 s) on the VDI; **71 passed** after adding the two seed-stage tests (dev environment, 148 s) |
| Line coverage of `job.py` | **98 %** (606 statements, 15 missed — defensive branches) |
| Report | `data/acme-performance/logs/test-report-<timestamp>.txt` (git-ignored) |

| Test file | What it proves | Kind |
|---|---|---|
| `tests/test_contracts.py` | Every Pydantic contract accepts valid rows and rejects each rule violation (ranges, enums, patterns, future dates, missing fields); every defect seen in the sample events (bad dates `N/A`/`yesterday`/`2026-13-45`/empty, missing or non-numeric employee, `notes`→`text`, malformed JSON) is handled; raw records are kept for quarantine | Unit |
| `tests/test_bronze_helpers.py` | Settings come only from environment variables; partition paths; metadata columns; SQL table allow-list; retry with exponential backoff (and no retry on unexpected errors); reconciliation rule; JSON log format | Unit |
| `tests/test_spark_transformations.py` | Silver de-duplication and foreign-key quarantine; **every Gold business rule** on hand-made data with known answers (rating change, gap severity, coverage %, promotion candidate, training counts, course status, attrition score, team/project join, skill levels); Gold grain checks fail on duplicates | Spark |
| `tests/test_integration_pipeline.py` | Real PostgreSQL + in-memory S3: Bronze row counts equal PostgreSQL counts; Bronze file sizes equal landing sizes; **re-running Bronze and Silver adds nothing**; Silver reconciles and quarantines; all 9 Gold tables pass grain + reconciliation; an unreachable database gives exit code 1 and a `FAILED` lineage manifest | Integration |

## 2. Pipeline runs on the VDI (LocalStack + PostgreSQL)

```bash
./data/acme-performance/run_local.sh --stage bronze   # then silver, then gold; each run twice
```

| Stage | Run 1 | Run 2 (re-run) | Conclusion |
|---|---|---|---|
| Bronze | 7 tables / 12,173 rows; 783 files copied (5.4 s) | 0 copied, 783 skipped; still 1 snapshot per table | Idempotent, incremental |
| Silver | 8 entities; 41,747 written; 142 quarantined; 152 duplicates removed (44.5 s) | identical counts; same number of files (45.6 s) | Idempotent; every entity reconciled |
| Gold | 9 tables; 39,549 rows (30.3 s) | identical (30.7 s) | Idempotent; all grain checks passed |

### Reconciliation (Silver manifest)

| Entity | Read | Duplicates | Rejected | Written | Reconciled |
|---|---|---|---|---|---|
| teams | 100 | 0 | 0 | 100 | ✅ |
| competencies | 40 | 0 | 0 | 40 | ✅ |
| employees | 2,000 | 0 | 0 | 2,000 | ✅ |
| projects | 33 | 0 | 0 | 33 | ✅ |
| performance_reviews | 4,000 | 0 | 0 | 4,000 | ✅ |
| competency_assessments | 4,000 | 0 | 0 | 4,000 | ✅ |
| talent_review | 2,000 | 0 | 0 | 2,000 | ✅ |
| learning_activity | 29,868 | 152 | 142 | 29,574 | ✅ |

Quarantine reasons (learning events): 76 invalid/missing `employee_id`, 53 invalid `event_date`,
13 malformed JSON (one record had two problems and is counted once).
Gold cross-check: learning events counted in Gold (`employee_training_summary`) = 29,574 = Silver.

### Insights notebook

```bash
./notebooks/run_notebook.sh
```
Executed on the VDI with 0 errors and 7 charts.

## 3. Performance (single VDI, local Spark)

| Measure | Value |
|---|---|
| Silver throughput | 41,741 input records in ~45 s ≈ **55,000 records/min** (includes Spark start-up ~10 s) |
| Bronze | 12,173 rows + 783 files in 5.4 s |
| End-to-end Bronze → Gold | ≈ 80 s |

The sample is small, so Spark start-up dominates. Not yet measured: memory/CPU profiling and
small/medium/large scale tests — the job can be pointed at a larger landing prefix with no code change.

## 4. Cloud deployment (AWS ap-south-1, 2026-10-15)

```bash
./data/acme-performance/deploy_cloud.sh plan     # Plan: 8 to add, 2 to change (Aurora tags only), 0 to destroy
./data/acme-performance/deploy_cloud.sh apply    # run by the participant
./data/acme-performance/deploy_cloud.sh status
```

| Check | Result |
|---|---|
| Data lake bucket + uploads | ✅ `Apply complete! 3 added`; `landing files in S3: 783`; seed CSVs uploaded |
| EKS, node group, JupyterHub, Helm data job | ✅ `Apply complete! Resources: 5 added, 2 changed, 0 destroyed`; Helm `STATUS: deployed` |
| Job on EKS | ❌ 4 pods, all `Error`: seed stage → `connection to server at "coding-workshop-rds-1bd9f4ac.cluster-…" (10.0.40.119), port 5432 failed: timeout expired`; retried 1 s/2 s/4 s, then `job failed` (retry and failure logging worked as designed) |
| Diagnosis | EKS nodes in `sg-03b8f8749f01e886e`; Aurora SG `sg-033177241dd1d5acc` allows only itself + TCP 389/443 |
| Permission check (`--dry-run`, nothing changed) | `AuthorizeSecurityGroupIngress` → UnauthorizedOperation; `CreateLaunchTemplate` → UnauthorizedOperation |

**Conclusion:** deployment automation works; the cloud run needs one security-group rule from the workshop admin
(TCP 5432 from the EKS cluster SG to the Aurora SG). See README → *Cloud deployment*.

## 5. Defects found by testing on the VDI (and fixed)

| Symptom | Cause | Fix |
|---|---|---|
| `java: No such file or directory` | VDI `~/.bashrc` sets `JAVA_HOME=/usr/lib/jvm/java-$JAVA_VERSION-openjdk-amd64` with `JAVA_VERSION` empty | `env_setup.sh` derives `JAVA_HOME` from `java` when invalid |
| `ModuleNotFoundError: pydantic` in Spark workers | Workers started the system `python3`, not the job's venv | `job.py` sets `PYSPARK_PYTHON` to the job's interpreter |
| PySpark 3.5.4 (template pin) unusable | Does not support Python 3.13; VDI has Spark 4.1.2 | Pinned `pyspark==4.1.2` |
