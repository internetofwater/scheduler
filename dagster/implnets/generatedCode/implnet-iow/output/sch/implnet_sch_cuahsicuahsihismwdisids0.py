from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihismwdisids0 import implnet_job_cuahsicuahsihismwdisids0

@schedule(cron_schedule="0 16 6 * *", job=implnet_job_cuahsicuahsihismwdisids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihismwdisids0(_context):
    run_config = {}
    return run_config
