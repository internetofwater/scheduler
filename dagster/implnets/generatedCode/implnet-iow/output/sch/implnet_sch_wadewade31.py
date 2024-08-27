from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade31 import implnet_job_wadewade31

@schedule(cron_schedule="0 4 11 * *", job=implnet_job_wadewade31, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade31(_context):
    run_config = {}
    return run_config
