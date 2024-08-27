from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade26 import implnet_job_wadewade26

@schedule(cron_schedule="0 14 10 * *", job=implnet_job_wadewade26, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade26(_context):
    run_config = {}
    return run_config
