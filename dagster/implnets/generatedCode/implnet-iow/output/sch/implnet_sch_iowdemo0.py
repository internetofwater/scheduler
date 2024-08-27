from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_iowdemo0 import implnet_job_iowdemo0

@schedule(cron_schedule="0 4 2 * *", job=implnet_job_iowdemo0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_iowdemo0(_context):
    run_config = {}
    return run_config
