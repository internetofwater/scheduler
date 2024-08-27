from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nmwdiose2 import implnet_job_nmwdiose2

@schedule(cron_schedule="0 20 25 * *", job=implnet_job_nmwdiose2, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nmwdiose2(_context):
    run_config = {}
    return run_config
