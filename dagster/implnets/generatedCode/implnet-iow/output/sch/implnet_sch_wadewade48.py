from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade48 import implnet_job_wadewade48

@schedule(cron_schedule="0 14 9 * *", job=implnet_job_wadewade48, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade48(_context):
    run_config = {}
    return run_config
