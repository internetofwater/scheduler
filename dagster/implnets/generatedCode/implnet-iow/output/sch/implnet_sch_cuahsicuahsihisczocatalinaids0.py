from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisczocatalinaids0 import implnet_job_cuahsicuahsihisczocatalinaids0

@schedule(cron_schedule="0 0 7 * *", job=implnet_job_cuahsicuahsihisczocatalinaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisczocatalinaids0(_context):
    run_config = {}
    return run_config
