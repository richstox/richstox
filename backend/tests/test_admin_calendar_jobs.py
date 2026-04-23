import ast
from functools import lru_cache
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from services.admin_overview_service import JOB_REGISTRY, get_next_run_datetime


CALENDAR_JOBS = {
    "dividend_upcoming_calendar": (4, 50),
    "earnings_upcoming_calendar": (4, 55),
    "splits_upcoming_calendar": (4, 57),
    "ipos_upcoming_calendar": (4, 58),
}


@lru_cache(maxsize=1)
def _get_server_job_sets():
    server_path = Path(__file__).resolve().parents[1] / "server.py"
    tree = ast.parse(server_path.read_text())

    always_runnable_jobs = set()
    manual_runner_jobs = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "_ALWAYS_RUNNABLE_JOBS" and isinstance(node.value, ast.Set):
                always_runnable_jobs = {
                    elt.value for elt in node.value.elts
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                }
            if isinstance(target, ast.Name) and target.id == "JOB_RUNNERS" and isinstance(node.value, ast.Dict):
                manual_runner_jobs = {
                    key.value for key in node.value.keys
                    if isinstance(key, ast.Constant) and isinstance(key.value, str)
                }

    return always_runnable_jobs, manual_runner_jobs


def test_calendar_jobs_present_in_admin_job_registry():
    for job_name, (hour, minute) in CALENDAR_JOBS.items():
        reg = JOB_REGISTRY.get(job_name)
        assert reg is not None, f"{job_name} missing from JOB_REGISTRY"
        assert reg["hour"] == hour
        assert reg["minute"] == minute
        assert reg["has_api_calls"] is True


def test_calendar_jobs_next_run_uses_registry_schedule():
    now = datetime(2026, 4, 23, 4, 40, tzinfo=ZoneInfo("Europe/Prague"))

    next_dividend = get_next_run_datetime("dividend_upcoming_calendar", now)
    next_ipo = get_next_run_datetime("ipos_upcoming_calendar", now)

    assert (next_dividend.hour, next_dividend.minute) == (4, 50)
    assert (next_ipo.hour, next_ipo.minute) == (4, 58)
    assert next_dividend.date() == now.date()
    assert next_ipo.date() == now.date()


def test_calendar_jobs_exposed_in_manual_run_and_status_maps():
    always_runnable_jobs, manual_runner_jobs = _get_server_job_sets()

    expected = set(CALENDAR_JOBS.keys())
    assert expected.issubset(always_runnable_jobs)
    assert expected.issubset(manual_runner_jobs)
