"""Shared pytest fixtures for the acme-performance job."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

JOB_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = JOB_DIR.parents[1]

# Make `import job` work in the test process *and* in Spark's Python workers.
sys.path.insert(0, str(JOB_DIR))
os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [str(JOB_DIR), os.environ.get("PYTHONPATH")]))


@pytest.fixture(scope="session")
def spark():
    """One local Spark session for all Spark tests (startup is slow)."""
    import job

    session = job.create_spark_session("acme-performance-tests")
    yield session
    session.stop()


@pytest.fixture
def run_context():
    """A fixed run context so outputs are predictable."""
    import job

    return job.RunContext.new()
