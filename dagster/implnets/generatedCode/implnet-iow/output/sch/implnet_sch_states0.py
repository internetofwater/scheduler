from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_states0 import implnet_job_states0

@schedule(cron_schedule="0 2 4 * *", job=implnet_job_states0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_states0(_context):
    run_config = {}
    return run_config
