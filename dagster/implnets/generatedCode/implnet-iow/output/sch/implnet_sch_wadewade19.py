from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade19 import implnet_job_wadewade19

@schedule(cron_schedule="0 6 10 * *", job=implnet_job_wadewade19, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade19(_context):
    run_config = {}
    return run_config
