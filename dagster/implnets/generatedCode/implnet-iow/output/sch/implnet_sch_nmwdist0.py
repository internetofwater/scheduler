from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nmwdist0 import implnet_job_nmwdist0

@schedule(cron_schedule="0 12 26 * *", job=implnet_job_nmwdist0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nmwdist0(_context):
    run_config = {}
    return run_config
