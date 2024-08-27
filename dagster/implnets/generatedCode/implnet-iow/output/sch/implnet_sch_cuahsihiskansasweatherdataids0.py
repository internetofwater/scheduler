from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihiskansasweatherdataids0 import implnet_job_cuahsihiskansasweatherdataids0

@schedule(cron_schedule="0 2 7 * *", job=implnet_job_cuahsihiskansasweatherdataids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihiskansasweatherdataids0(_context):
    run_config = {}
    return run_config
