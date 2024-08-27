from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refhu08hu080 import implnet_job_refhu08hu080

@schedule(cron_schedule="0 22 3 * *", job=implnet_job_refhu08hu080, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refhu08hu080(_context):
    run_config = {}
    return run_config
