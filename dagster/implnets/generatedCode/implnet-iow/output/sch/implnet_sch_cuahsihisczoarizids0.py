from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisczoarizids0 import implnet_job_cuahsihisczoarizids0

@schedule(cron_schedule="0 16 18 * *", job=implnet_job_cuahsihisczoarizids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisczoarizids0(_context):
    run_config = {}
    return run_config
