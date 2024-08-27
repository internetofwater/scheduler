from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisnevadosids0 import implnet_job_cuahsicuahsihisnevadosids0

@schedule(cron_schedule="0 0 2 * *", job=implnet_job_cuahsicuahsihisnevadosids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisnevadosids0(_context):
    run_config = {}
    return run_config
