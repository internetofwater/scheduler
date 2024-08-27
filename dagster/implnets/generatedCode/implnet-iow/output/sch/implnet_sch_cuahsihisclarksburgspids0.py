from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisclarksburgspids0 import implnet_job_cuahsihisclarksburgspids0

@schedule(cron_schedule="0 4 5 * *", job=implnet_job_cuahsihisclarksburgspids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisclarksburgspids0(_context):
    run_config = {}
    return run_config
