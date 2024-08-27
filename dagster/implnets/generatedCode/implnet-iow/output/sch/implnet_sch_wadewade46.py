from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade46 import implnet_job_wadewade46

@schedule(cron_schedule="0 22 10 * *", job=implnet_job_wadewade46, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade46(_context):
    run_config = {}
    return run_config
