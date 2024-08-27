from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade42 import implnet_job_wadewade42

@schedule(cron_schedule="0 20 9 * *", job=implnet_job_wadewade42, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade42(_context):
    run_config = {}
    return run_config
