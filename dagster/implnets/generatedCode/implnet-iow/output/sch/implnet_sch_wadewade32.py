from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade32 import implnet_job_wadewade32

@schedule(cron_schedule="0 4 9 * *", job=implnet_job_wadewade32, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade32(_context):
    run_config = {}
    return run_config
