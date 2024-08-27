from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisczoboulderids0 import implnet_job_cuahsihisczoboulderids0

@schedule(cron_schedule="0 4 8 * *", job=implnet_job_cuahsihisczoboulderids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisczoboulderids0(_context):
    run_config = {}
    return run_config
