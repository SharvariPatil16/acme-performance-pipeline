"""Unit tests for the Silver schema contracts (Pydantic) and event parsing."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

import job

VALID = {
    job.Team: {"team_id": 1, "name": "Team A", "department": "Sales"},
    job.Competency: {"competency_id": 1, "name": "Python", "category": "Technical"},
    job.Employee: {"employee_id": 1, "first_name": "Ana", "last_name": "Lee", "email": "Ana.Lee@acme.example",
                   "team_id": 1, "job_level": "IC2", "hire_date": date(2020, 1, 1)},
    job.Project: {"project_id": 1, "name": "Launch", "team_id": 1, "outcome_status": "on-time",
                  "outcome_score": Decimal("56.87")},
    job.PerformanceReview: {"review_id": 1, "employee_id": 1, "review_period": "2025-H1", "rating": 4,
                            "manager_comment": None, "submitted_date": date(2025, 8, 1)},
    job.CompetencyAssessment: {"assessment_id": 1, "employee_id": 1, "competency_id": 1, "proficiency_level": 3,
                               "required_level": 4, "assessed_date": date(2025, 9, 1)},
    job.TalentReview: {"employee_id": 1, "review_cycle": "2025-H1", "potential_rating": "High",
                       "promotion_readiness": "Ready Now", "attrition_risk": "Low"},
}

EVENT = {"event_id": "EV0000013453", "employee_id": "1178", "event_date": "2025-01-10T14:42:00",
         "event_type": "module_completed", "course_name": "Presentation Skills", "text": "Finished.", "score": None}


@pytest.mark.parametrize("model", list(VALID))
def test_valid_records_pass(model):
    ok, clean = job.validate_record(VALID[model], model)
    assert ok, clean


@pytest.mark.parametrize(
    ("model", "field", "bad_value"),
    [
        (job.Team, "name", ""),
        (job.Competency, "category", "Soft Skills"),
        (job.Employee, "email", "not-an-email"),
        (job.Employee, "team_id", 0),
        (job.Employee, "hire_date", date.today() + timedelta(days=30)),
        (job.Project, "outcome_status", "done"),
        (job.Project, "outcome_score", Decimal("100.01")),
        (job.PerformanceReview, "rating", 6),
        (job.PerformanceReview, "rating", 0),
        (job.PerformanceReview, "review_period", "2025-Q1"),
        (job.CompetencyAssessment, "proficiency_level", 7),
        (job.TalentReview, "promotion_readiness", "Soon"),
        (job.TalentReview, "attrition_risk", "Extreme"),
    ],
)
def test_contract_rejects_rule_violations(model, field, bad_value):
    ok, rejection = job.validate_record({**VALID[model], field: bad_value}, model)
    assert not ok
    assert rejection["reason"] == "schema_validation"
    assert any(err.startswith(field) for err in rejection["errors"]), rejection["errors"]
    assert rejection["record"][field] == bad_value  # raw record kept for quarantine


def test_missing_required_field_is_rejected():
    record = dict(VALID[job.Employee])
    del record["email"]
    ok, rejection = job.validate_record(record, job.Employee)
    assert not ok and rejection["errors"][0].startswith("email")


def test_employee_email_is_lowercased_and_strings_trimmed():
    ok, clean = job.validate_record({**VALID[job.Employee], "first_name": "  Ana  "}, job.Employee)
    assert ok
    assert clean["email"] == "ana.lee@acme.example"
    assert clean["first_name"] == "Ana"


def test_bronze_metadata_columns_are_ignored():
    ok, clean = job.validate_record({**VALID[job.Team], "_ingest_run_id": "r1"}, job.Team)
    assert ok and "_ingest_run_id" not in clean


# --- Learning events: one test per real defect found in the sample data -----------------
def test_valid_event_is_conformed():
    ok, clean = job.validate_record(EVENT, job.LearningEvent)
    assert ok
    assert clean["employee_id"] == 1178                       # string id cast to int
    assert clean["event_date"] == datetime(2025, 1, 10, 14, 42, tzinfo=timezone.utc)  # stored as UTC


def test_notes_field_is_conformed_to_text():
    record = {k: v for k, v in EVENT.items() if k != "text"} | {"notes": "Passed first try."}
    ok, clean = job.validate_record(record, job.LearningEvent)
    assert ok and clean["text"] == "Passed first try."


@pytest.mark.parametrize("bad_date", ["N/A", "yesterday", "2026-13-45", ""])
def test_invalid_event_dates_are_rejected(bad_date):
    ok, rejection = job.validate_record({**EVENT, "event_date": bad_date}, job.LearningEvent)
    assert not ok and rejection["errors"][0].startswith("event_date")


@pytest.mark.parametrize("bad_employee", [None, "", "abc"])
def test_missing_or_invalid_employee_is_rejected(bad_employee):
    ok, rejection = job.validate_record({**EVENT, "employee_id": bad_employee}, job.LearningEvent)
    assert not ok and rejection["errors"][0].startswith("employee_id")


@pytest.mark.parametrize(("field", "value"), [("score", 101), ("score", -1), ("event_type", "lunch"),
                                              ("event_id", "X123")])
def test_event_domain_rules(field, value):
    ok, _ = job.validate_record({**EVENT, field: value}, job.LearningEvent)
    assert not ok


def test_malformed_json_line_is_rejected_with_raw_line():
    line = '{"event_id": "EV0000015950", "employee_id": "263", "event_type": "quiz_attempted",'
    ok, rejection = job.parse_event_line(line)
    assert not ok
    assert rejection["reason"] == "malformed_json"
    assert rejection["record"]["raw_line"] == line


def test_non_object_json_is_rejected():
    ok, rejection = job.parse_event_line("[1, 2, 3]")
    assert not ok and rejection["reason"] == "malformed_json"


def test_validate_event_line_keeps_source_file_for_lineage():
    import json

    ok, clean = job.validate_event_line(json.dumps(EVENT), "bronze/learning_activity/year=2025/x.jsonl")
    assert ok and clean["_source_file"] == "bronze/learning_activity/year=2025/x.jsonl"
    ok, rejection = job.validate_event_line("{broken", "bronze/learning_activity/year=2025/y.jsonl")
    assert not ok and rejection["_source_file"].endswith("y.jsonl")


def test_spark_schema_matches_contract():
    schema = job.spark_schema(job.LearningEvent)
    fields = {f.name: f for f in schema.fields}
    assert fields["employee_id"].dataType.typeName() == "long" and not fields["employee_id"].nullable
    assert fields["event_date"].dataType.typeName() == "timestamp"
    assert fields["score"].nullable and fields["text"].nullable
    assert job.spark_schema(job.Project).fields[-1].dataType.typeName() == "decimal"
