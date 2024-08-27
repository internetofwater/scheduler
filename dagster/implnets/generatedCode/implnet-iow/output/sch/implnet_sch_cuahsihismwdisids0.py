from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihismwdisids0 import implnet_job_cuahsihismwdisids0

@schedule(cron_schedule="0 0 1 * *", job=implnet_job_cuahsihismwdisids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihismwdisids0(_context):
    run_config = {}
    return run_config
