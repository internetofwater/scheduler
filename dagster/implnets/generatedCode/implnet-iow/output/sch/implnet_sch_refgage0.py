from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refgage0 import implnet_job_refgage0

@schedule(cron_schedule="0 20 22 * *", job=implnet_job_refgage0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refgage0(_context):
    run_config = {}
    return run_config
