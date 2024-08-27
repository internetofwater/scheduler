from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisirwaids0 import implnet_job_cuahsicuahsihisirwaids0

@schedule(cron_schedule="0 6 7 * *", job=implnet_job_cuahsicuahsihisirwaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisirwaids0(_context):
    run_config = {}
    return run_config
