from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade43 import implnet_job_wadewade43

@schedule(cron_schedule="0 10 9 * *", job=implnet_job_wadewade43, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade43(_context):
    run_config = {}
    return run_config
