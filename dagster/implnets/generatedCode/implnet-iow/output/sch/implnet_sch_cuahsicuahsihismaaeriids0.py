from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihismaaeriids0 import implnet_job_cuahsicuahsihismaaeriids0

@schedule(cron_schedule="0 20 7 * *", job=implnet_job_cuahsicuahsihismaaeriids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihismaaeriids0(_context):
    run_config = {}
    return run_config
