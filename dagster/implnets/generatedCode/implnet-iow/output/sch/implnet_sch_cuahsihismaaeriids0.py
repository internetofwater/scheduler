from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihismaaeriids0 import implnet_job_cuahsihismaaeriids0

@schedule(cron_schedule="0 20 7 * *", job=implnet_job_cuahsihismaaeriids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihismaaeriids0(_context):
    run_config = {}
    return run_config
