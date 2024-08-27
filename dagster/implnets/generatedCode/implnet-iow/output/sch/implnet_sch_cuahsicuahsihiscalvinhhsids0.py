from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihiscalvinhhsids0 import implnet_job_cuahsicuahsihiscalvinhhsids0

@schedule(cron_schedule="0 14 2 * *", job=implnet_job_cuahsicuahsihiscalvinhhsids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihiscalvinhhsids0(_context):
    run_config = {}
    return run_config
