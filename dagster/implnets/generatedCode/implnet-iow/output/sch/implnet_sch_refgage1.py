from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refgage1 import implnet_job_refgage1

@schedule(cron_schedule="0 8 23 * *", job=implnet_job_refgage1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refgage1(_context):
    run_config = {}
    return run_config
