"""Spark tests: Silver de-duplication / foreign keys and every Gold business rule.

Uses tiny hand-made datasets so each expected value can be checked by eye.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

import job

pytestmark = pytest.mark.spark


def df(spark, rows, model, extra=()):
    """Build a DataFrame with the Silver schema of ``model``."""
    schema = job.spark_schema(model, extra)
    return spark.createDataFrame([tuple(r.get(f) for f in schema.fieldNames()) for r in rows], schema)


@pytest.fixture
def silver(spark):
    """A miniature Silver layer: 2 teams, 3 employees, 2 competencies."""
    teams = [{"team_id": 1, "name": "T1", "department": "Sales"},
             {"team_id": 2, "name": "T2", "department": "Design"}]
    employees = [{"employee_id": e, "first_name": f"E{e}", "last_name": "X", "email": f"e{e}@a.io",
                  "team_id": t, "job_level": "IC2", "hire_date": date(2020, 1, 1)}
                 for e, t in [(1, 1), (2, 1), (3, 2)]]
    competencies = [{"competency_id": 1, "name": "Python", "category": "Technical"},
                    {"competency_id": 2, "name": "Negotiation", "category": "Leadership"}]
    projects = [{"project_id": 1, "name": "P1", "team_id": 1, "outcome_status": "on-time", "outcome_score": Decimal("80.00")},
                {"project_id": 2, "name": "P2", "team_id": 1, "outcome_status": "delayed", "outcome_score": Decimal("40.00")}]
    reviews = []
    for emp, (h2, h1) in {1: (3, 5), 2: (4, 2), 3: (3, 3)}.items():
        reviews += [{"review_id": emp * 10 + 1, "employee_id": emp, "review_period": "2024-H2", "rating": h2,
                     "submitted_date": date(2025, 8, 1)},
                    {"review_id": emp * 10 + 2, "employee_id": emp, "review_period": "2025-H1", "rating": h1,
                     "submitted_date": date(2026, 2, 1)}]
    assessments = [  # employee 1: gap 0 ; employee 2: gap 2 (critical) and 1 (minor) ; employee 3: gap 3
        {"assessment_id": 1, "employee_id": 1, "competency_id": 1, "proficiency_level": 4, "required_level": 4},
        {"assessment_id": 2, "employee_id": 2, "competency_id": 1, "proficiency_level": 2, "required_level": 4},
        {"assessment_id": 3, "employee_id": 2, "competency_id": 2, "proficiency_level": 3, "required_level": 4},
        {"assessment_id": 4, "employee_id": 3, "competency_id": 2, "proficiency_level": 1, "required_level": 4},
    ]
    for a in assessments:
        a["assessed_date"] = date(2025, 9, 1)
    talent = [{"employee_id": 1, "review_cycle": "2025-H1", "potential_rating": "High",
               "promotion_readiness": "Ready Now", "attrition_risk": "Low"},
              {"employee_id": 2, "review_cycle": "2025-H1", "potential_rating": "High",
               "promotion_readiness": "Ready Now", "attrition_risk": "High"},
              {"employee_id": 3, "review_cycle": "2025-H1", "potential_rating": "Low",
               "promotion_readiness": "Not Ready", "attrition_risk": "Medium"}]
    events = [  # employee 1: enrolled + completed + certified recently; employee 2: one old event; 3: none
        {"event_id": "EV0000000001", "employee_id": 1, "event_date": datetime(2026, 9, 1), "event_type": "enrollment",
         "course_name": "Py"},
        {"event_id": "EV0000000002", "employee_id": 1, "event_date": datetime(2026, 9, 1), "event_type": "course_completed",
         "course_name": "Py"},
        {"event_id": "EV0000000003", "employee_id": 1, "event_date": datetime(2026, 9, 2), "event_type": "certification_earned",
         "course_name": "Py"},
        {"event_id": "EV0000000004", "employee_id": 2, "event_date": datetime(2024, 1, 1), "event_type": "enrollment",
         "course_name": "SQL"},
    ]
    return {
        "teams": df(spark, teams, job.Team), "employees": df(spark, employees, job.Employee),
        "competencies": df(spark, competencies, job.Competency), "projects": df(spark, projects, job.Project),
        "performance_reviews": df(spark, reviews, job.PerformanceReview),
        "competency_assessments": df(spark, assessments, job.CompetencyAssessment),
        "talent_review": df(spark, talent, job.TalentReview), "learning_activity": df(spark, events, job.LearningEvent),
    }


def by_id(dataframe, key="employee_id"):
    return {row[key]: row.asDict() for row in dataframe.collect()}


# --- Silver ------------------------------------------------------------------------------
def test_apply_contract_dedupes_and_quarantines_orphans(spark, silver, run_context):
    records = spark.sparkContext.parallelize([
        (True, {"review_id": 1, "employee_id": 1, "review_period": "2025-H1", "rating": 4, "submitted_date": date(2026, 1, 1)}),
        (True, {"review_id": 1, "employee_id": 1, "review_period": "2025-H1", "rating": 4, "submitted_date": date(2026, 1, 1)}),
        (True, {"review_id": 2, "employee_id": 99, "review_period": "2025-H1", "rating": 3, "submitted_date": date(2026, 1, 1)}),
        (False, {"reason": "schema_validation", "errors": ["rating: too big"], "record": {"review_id": 3}}),
    ])
    spec = next(s for s in job.TABLE_SPECS if s.name == "performance_reviews")
    valid, rejected, duplicates = job.apply_contract(spark, spec, records, silver, run_context, extra=[])
    assert valid.count() == 1 and duplicates == 1
    assert sorted(r["reason"] for r in rejected) == ["schema_validation", "unknown_employee_id"]
    assert {"_silver_run_id", "_silver_processed_at"} <= set(valid.columns)


# --- Gold: one test per business rule ---------------------------------------------------
def test_performance_history_rating_change(silver):
    rows = job.build_performance_history(silver).filter("review_period = '2025-H1'")
    changes = {r["employee_id"]: r["rating_change"] for r in rows.collect()}
    assert changes == {1: 2, 2: -2, 3: 0}


def test_skill_gap_severity(silver):
    gaps = {(r["employee_id"], r["competency_id"]): r for r in job.build_skill_gaps(silver).collect()}
    assert gaps[(1, 1)]["gap_severity"] == "none" and gaps[(1, 1)]["gap_levels"] == 0
    assert gaps[(2, 1)]["gap_severity"] == "critical" and gaps[(2, 1)]["is_critical_gap"]
    assert gaps[(2, 2)]["gap_severity"] == "minor"
    assert gaps[(3, 2)]["gap_levels"] == 3


def test_competency_coverage(silver):
    coverage = by_id(job.build_competency_coverage(job.build_skill_gaps(silver)), "competency_id")
    assert coverage[1]["critical_gap_count"] == 1 and coverage[1]["pct_meeting_required"] == 50.0
    assert coverage[2]["critical_gap_count"] == 1 and coverage[2]["employees_assessed"] == 2


def test_promotion_candidate_rule(silver):
    promo = by_id(job.build_promotion_readiness(silver, job.build_skill_gaps(silver)))
    assert promo[1]["is_promotion_candidate"]          # High + Ready Now + rating 5 + no gaps
    assert not promo[2]["is_promotion_candidate"]      # rating fell to 2 and has a critical gap
    assert not promo[3]["is_promotion_candidate"]      # low potential
    assert promo[2]["rating_trend"] == "declining" and promo[1]["rating_trend"] == "improving"


def test_training_summary_includes_everyone(silver):
    training = by_id(job.build_training_summary(silver))
    assert len(training) == 3
    assert training[1]["courses_completed"] == 1 and training[1]["certifications_earned"] == 1
    assert training[1]["active_last_12_months"] and not training[2]["active_last_12_months"]
    assert training[3]["learning_events"] == 0 and not training[3]["active_last_12_months"]


def test_course_progress_status(silver):
    progress = {(r["employee_id"], r["course_name"]): r["status"]
                for r in job.build_course_progress(silver).collect()}
    assert progress == {(1, "Py"): "certified", (2, "SQL"): "in progress"}


def test_attrition_risk_score(silver):
    training = job.build_training_summary(silver)
    risk = by_id(job.build_attrition_risk(silver, training))
    # employee 2: label High (3) + declining (1) + low rating (1) + inactive (1) = 6
    assert risk[2]["risk_score"] == 6 and risk[2]["risk_band"] == "High"
    # employee 1: label Low (1), improving, rating 5, active = 1
    assert risk[1]["risk_score"] == 1 and risk[1]["risk_band"] == "Low"
    # employee 3: label Medium (2) + inactive (1) = 3
    assert risk[3]["risk_score"] == 3 and risk[3]["risk_band"] == "Medium"


def test_team_performance_joins_projects(silver):
    promo = job.build_promotion_readiness(silver, job.build_skill_gaps(silver))
    teams = by_id(job.build_team_performance(silver, promo, job.build_skill_gaps(silver)), "team_id")
    assert teams[1]["projects"] == 2 and float(teams[1]["avg_project_outcome_score"]) == 60.0
    assert teams[1]["pct_projects_on_time"] == 50.0 and teams[1]["headcount"] == 2
    assert teams[2]["projects"] == 0 and not teams[2]["has_projects"]


def test_skill_distribution_levels(silver):
    dist = {(r["department"], r["competency_id"]): r
            for r in job.build_skill_distribution(job.build_skill_gaps(silver)).collect()}
    assert dist[("Sales", 1)]["level_4"] == 1 and dist[("Sales", 1)]["level_2"] == 1
    assert dist[("Design", 2)]["level_1"] == 1


def test_gold_checks_catch_duplicate_grain(spark):
    duplicated = spark.createDataFrame([(1,), (1,)], "employee_id long")
    with pytest.raises(RuntimeError, match="grain_unique"):
        job.gold_checks("t", duplicated, ("employee_id",), expected_rows=None)
    with pytest.raises(RuntimeError, match="rows_match_expected"):
        job.gold_checks("t", spark.createDataFrame([(1,)], "employee_id long"), ("employee_id",), expected_rows=2)
