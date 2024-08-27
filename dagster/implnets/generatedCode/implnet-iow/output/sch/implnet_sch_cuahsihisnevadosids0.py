from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisnevadosids0 import implnet_job_cuahsihisnevadosids0

@schedule(cron_schedule="0 0 13 * *", job=implnet_job_cuahsihisnevadosids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisnevadosids0(_context):
    run_config = {}
    return run_config
