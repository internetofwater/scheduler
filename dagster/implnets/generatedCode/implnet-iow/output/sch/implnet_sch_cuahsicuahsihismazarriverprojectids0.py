from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihismazarriverprojectids0 import implnet_job_cuahsicuahsihismazarriverprojectids0

@schedule(cron_schedule="0 0 3 * *", job=implnet_job_cuahsicuahsihismazarriverprojectids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihismazarriverprojectids0(_context):
    run_config = {}
    return run_config
