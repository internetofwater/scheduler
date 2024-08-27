from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade8 import implnet_job_wadewade8

@schedule(cron_schedule="0 18 9 * *", job=implnet_job_wadewade8, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade8(_context):
    run_config = {}
    return run_config
