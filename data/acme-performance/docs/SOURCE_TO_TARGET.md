# Source-to-target mapping

Where every column comes from and exactly where it is **renamed**, **cast**, **derived** or **aggregated**.
Function names refer to `data/acme-performance/job.py`.

Legend: **=** copied unchanged · **cast** type changed · **rename** · **derived** computed per row ·
**agg** aggregated · **meta** pipeline metadata (lineage)

## 1. Source → Bronze (`ingest_into_bronze`)

No business transformation in Bronze. Every source column is copied unchanged (=).

| Source | Bronze target | Added columns (meta) |
|---|---|---|
| PostgreSQL `public.<table>` (7 tables) | `bronze/postgres/<table>/ingest_date=YYYY-MM-DD/<table>.parquet` | `_ingest_run_id`, `_ingested_at` (UTC ISO), `_source_system`=`postgres`, `_source_object`=`<db>.public.<table>` |
| S3 `landing/learning_activity/year=/month=/day=/events-NNN.jsonl` | `bronze/learning_activity/year=/month=/day=/events-NNN.jsonl` (same bytes) | S3 object metadata: `ingest-run-id`, `ingested-at`, `source-system`=`s3-landing`, `source-key` |

Parquet keeps the PostgreSQL types (`INTEGER`→int64, `TEXT`→string, `DATE`→date, `NUMERIC(5,2)`→decimal).

## 2. Bronze → Silver (`transform_to_silver`, contracts in the *Silver layer: schema contracts* section)

All entities: strings are trimmed (**derived**); Bronze metadata columns are dropped and replaced by
`_bronze_run_id` (= Bronze `_ingest_run_id`) for tables or `_source_file` for events, plus `_silver_run_id` and
`_silver_processed_at` (**meta**). Rows failing the contract or a foreign key go to `silver/_quarantine/`.

| Silver entity | Column | From Bronze | Rule |
|---|---|---|---|
| `teams` | team_id, name, department | = | id > 0; text non-empty |
| `competencies` | competency_id, name, category | = | category ∈ {Technical, Leadership, Domain} |
| `employees` | employee_id, first_name, last_name, team_id, job_level, hire_date | = | team_id must exist in teams; hire_date ≤ today |
| `employees` | email | **derived** lower-case | must match `x@y.z` |
| `projects` | project_id, name, team_id, outcome_status | = | status ∈ {on-time, delayed, at-risk, cancelled}; team must exist |
| `projects` | outcome_score | **cast** decimal(5,2) | 0 – 100 |
| `performance_reviews` | review_id, employee_id, review_period, rating, manager_comment, submitted_date | = | rating 1–5; period `YYYY-H1/H2`; employee must exist |
| `competency_assessments` | assessment_id, employee_id, competency_id, proficiency_level, required_level, assessed_date | = | levels 1–5; employee and competency must exist |
| `talent_review` | employee_id, review_cycle, potential_rating, promotion_readiness, attrition_risk | = | enums from schema.sql; employee must exist |
| `learning_activity` | (raw JSON line) | **parsed** `json.loads` | malformed JSON → quarantine `malformed_json` |
| `learning_activity` | event_id | = | pattern `EV` + 10 digits; duplicates dropped |
| `learning_activity` | employee_id | **cast** string → long | required, > 0, must exist in employees |
| `learning_activity` | event_date | **cast** string → timestamp, **UTC** | invalid dates (`N/A`, `yesterday`, `2026-13-45`, empty) → quarantine |
| `learning_activity` | event_type, course_name | = | event_type ∈ 7 known types |
| `learning_activity` | text | **rename** `notes` → `text` when `text` is missing | schema drift conformed |
| `learning_activity` | score | **cast** → long (nullable) | 0 – 100 |
| `learning_activity` | event_year, event_month | **derived** from event_date | partition columns |

Exact duplicate event lines are removed **before** parsing (152 in the sample).

## 3. Silver → Gold (`aggregate_to_gold`)

Shared enrichment `employee_dim`: `employee_name` = **derived** `first_name || ' ' || last_name`; `team_name`
= **rename** of `teams.name`; `department`, `job_level`, `hire_date`, `team_id` = joined from employees/teams.
Every Gold table also gets `_gold_run_id`, `_gold_built_at` (**meta**).

### `employee_performance_history` — grain: employee_id × review_period (`build_performance_history`)
| Column | Source | Transformation |
|---|---|---|
| employee_id, review_period, rating, submitted_date, manager_comment | silver.performance_reviews | = |
| previous_rating | performance_reviews.rating | **derived** `lag(rating)` ordered by review_period per employee |
| rating_change | rating − previous_rating | **derived** |
| review_number | — | **derived** `row_number()` per employee |
| employee_name, job_level, hire_date, team_id, team_name, department | employee_dim | join |

### `employee_skill_gaps` — grain: employee_id × competency_id (`build_skill_gaps`)
| Column | Source | Transformation |
|---|---|---|
| employee_id, competency_id, proficiency_level, required_level, assessed_date | silver.competency_assessments | = |
| gap_levels | required_level − proficiency_level | **derived**, floored at 0 |
| gap_severity | gap | **derived** `critical` (≥ 2) / `minor` (1) / `none` |
| is_critical_gap | gap ≥ `CRITICAL_GAP_LEVELS` (2) | **derived** |
| competency_name, category | silver.competencies (`name` → **rename**) | join |
| employee/team columns | employee_dim | join |

### `competency_coverage` — grain: competency_id (`build_competency_coverage`)
| Column | Transformation (over employee_skill_gaps) |
|---|---|
| competency_id, competency_name, category | group key |
| employees_assessed | **agg** count distinct employee_id |
| avg_proficiency, avg_required | **agg** mean, 2 dp |
| pct_meeting_required | **agg** % rows with gap_levels = 0 |
| critical_gap_count | **agg** sum is_critical_gap |

### `promotion_readiness` — grain: employee_id (`build_promotion_readiness`)
| Column | Source | Transformation |
|---|---|---|
| review_cycle, potential_rating, promotion_readiness | silver.talent_review | = |
| latest_period, latest_rating, previous_rating | performance_reviews | **derived** newest / second-newest review per employee |
| avg_rating, reviews | performance_reviews | **agg** mean (2 dp), count |
| rating_trend | latest − previous | **derived** declining / improving / stable / single review |
| critical_gaps | employee_skill_gaps | **agg** sum is_critical_gap (0 if none) |
| is_high_potential | potential_rating = High | **derived** |
| is_promotion_candidate | High ∧ Ready Now ∧ latest_rating ≥ 4 ∧ critical_gaps = 0 | **derived** |

### `employee_training_summary` — grain: employee_id, all employees (`build_training_summary`)
| Column | Transformation (over silver.learning_activity) |
|---|---|
| learning_events | **agg** count (0 if none) |
| enrollments, modules_completed, courses_completed, quizzes_passed, certifications_earned | **agg** count by event_type |
| distinct_courses_completed | **agg** count distinct course_name where course_completed |
| avg_quiz_score | **agg** mean score of quiz events, 1 dp |
| last_activity_date | **agg** max date(event_date) |
| as_of_date | max date(event_date) over all events (data as-of date) |
| days_since_last_activity | **derived** as_of_date − last_activity_date |
| active_last_12_months | **derived** days_since_last_activity ≤ 365 (false if never active) |

### `employee_course_progress` — grain: employee_id × course_name (`build_course_progress`)
| Column | Transformation |
|---|---|
| first_activity, last_activity | **agg** min / max event_date |
| enrolled, completed, certified | **agg** any event of that type |
| best_score | **agg** max score |
| status | **derived** certified > completed > in progress |

### `attrition_risk` — grain: employee_id (`build_attrition_risk`)
| Column | Source | Transformation |
|---|---|---|
| attrition_risk, potential_rating | silver.talent_review | = |
| latest/previous/avg rating, rating_trend | as in promotion_readiness | derived |
| active_last_12_months | employee_training_summary | join |
| is_declining, is_low_performer | rating_trend = declining; latest_rating ≤ 2 | **derived** |
| risk_score | label (Low 1 / Medium 2 / High 3) + declining + low performer + inactive | **derived** |
| risk_band | score ≥ 4 High, = 3 Medium, else Low | **derived** |

### `team_performance` — grain: team_id (`build_team_performance`)
| Column | Source | Transformation |
|---|---|---|
| team_id, team_name, department | silver.teams | = / rename |
| headcount, avg_latest_rating, pct_high_performers (rating ≥ 4), pct_high_potential, avg_critical_gaps | promotion_readiness | **agg** per team |
| projects, avg_project_outcome_score, pct_projects_on_time | silver.projects | **agg** per team (0 projects if none) |
| has_projects | projects > 0 | **derived** |

### `skill_distribution` — grain: department × competency_id (`build_skill_distribution`)
| Column | Transformation (over employee_skill_gaps) |
|---|---|
| employees_assessed | **agg** count distinct employee_id |
| level_1 … level_5 | **agg** count of assessments at each proficiency level |
| avg_proficiency, pct_meeting_required, critical_gaps | **agg** |
