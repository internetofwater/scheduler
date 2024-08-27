from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_autotest10 import implnet_job_autotest10

@schedule(cron_schedule="0 4 27 * *", job=implnet_job_autotest10, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_autotest10(_context):
    run_config = {}
    return run_config
