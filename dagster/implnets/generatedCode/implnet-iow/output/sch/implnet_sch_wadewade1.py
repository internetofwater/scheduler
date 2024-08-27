from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade1 import implnet_job_wadewade1

@schedule(cron_schedule="0 2 11 * *", job=implnet_job_wadewade1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade1(_context):
    run_config = {}
    return run_config
