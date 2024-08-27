from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihismwraids0 import implnet_job_cuahsicuahsihismwraids0

@schedule(cron_schedule="0 10 2 * *", job=implnet_job_cuahsicuahsihismwraids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihismwraids0(_context):
    run_config = {}
    return run_config
