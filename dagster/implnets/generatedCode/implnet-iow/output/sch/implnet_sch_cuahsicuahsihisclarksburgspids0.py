from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisclarksburgspids0 import implnet_job_cuahsicuahsihisclarksburgspids0

@schedule(cron_schedule="0 4 5 * *", job=implnet_job_cuahsicuahsihisclarksburgspids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisclarksburgspids0(_context):
    run_config = {}
    return run_config
