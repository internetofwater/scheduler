from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihismazarriverprojectids0 import implnet_job_cuahsihismazarriverprojectids0

@schedule(cron_schedule="0 0 3 * *", job=implnet_job_cuahsihismazarriverprojectids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihismazarriverprojectids0(_context):
    run_config = {}
    return run_config
