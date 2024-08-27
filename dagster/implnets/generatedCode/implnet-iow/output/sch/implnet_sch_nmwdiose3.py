from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nmwdiose3 import implnet_job_nmwdiose3

@schedule(cron_schedule="0 6 8 * *", job=implnet_job_nmwdiose3, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nmwdiose3(_context):
    run_config = {}
    return run_config
