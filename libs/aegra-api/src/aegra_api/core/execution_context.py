from contextvars import ContextVar

from aegra_api.models.run_job import RunJob

current_job: ContextVar[RunJob] = ContextVar("aegra_current_job")
