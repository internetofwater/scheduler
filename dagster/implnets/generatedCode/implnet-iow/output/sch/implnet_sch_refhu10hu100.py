from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refhu10hu100 import implnet_job_refhu10hu100

@schedule(cron_schedule="0 16 4 * *", job=implnet_job_refhu10hu100, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refhu10hu100(_context):
    run_config = {}
    return run_config
