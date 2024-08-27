from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisczoarizids0 import implnet_job_cuahsicuahsihisczoarizids0

@schedule(cron_schedule="0 16 5 * *", job=implnet_job_cuahsicuahsihisczoarizids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisczoarizids0(_context):
    run_config = {}
    return run_config
