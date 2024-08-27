from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_autotest20 import implnet_job_autotest20

@schedule(cron_schedule="0 16 27 * *", job=implnet_job_autotest20, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_autotest20(_context):
    run_config = {}
    return run_config
