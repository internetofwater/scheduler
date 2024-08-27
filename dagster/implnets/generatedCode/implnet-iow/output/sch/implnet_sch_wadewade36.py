from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade36 import implnet_job_wadewade36

@schedule(cron_schedule="0 0 11 * *", job=implnet_job_wadewade36, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade36(_context):
    run_config = {}
    return run_config
