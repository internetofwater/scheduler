from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihiskansasweatherdataids0 import implnet_job_cuahsicuahsihiskansasweatherdataids0

@schedule(cron_schedule="0 2 7 * *", job=implnet_job_cuahsicuahsihiskansasweatherdataids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihiskansasweatherdataids0(_context):
    run_config = {}
    return run_config
