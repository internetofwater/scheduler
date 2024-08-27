from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nmwdistnmwdist0 import implnet_job_nmwdistnmwdist0

@schedule(cron_schedule="0 2 8 * *", job=implnet_job_nmwdistnmwdist0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nmwdistnmwdist0(_context):
    run_config = {}
    return run_config
