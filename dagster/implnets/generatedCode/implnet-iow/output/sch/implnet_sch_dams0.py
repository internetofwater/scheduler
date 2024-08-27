from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_dams0 import implnet_job_dams0

@schedule(cron_schedule="0 6 3 * *", job=implnet_job_dams0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_dams0(_context):
    run_config = {}
    return run_config
