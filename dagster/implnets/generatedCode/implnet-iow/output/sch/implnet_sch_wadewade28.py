from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade28 import implnet_job_wadewade28

@schedule(cron_schedule="0 8 12 * *", job=implnet_job_wadewade28, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade28(_context):
    run_config = {}
    return run_config
