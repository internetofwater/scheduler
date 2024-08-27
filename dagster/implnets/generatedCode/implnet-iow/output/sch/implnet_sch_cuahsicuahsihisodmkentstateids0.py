from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisodmkentstateids0 import implnet_job_cuahsicuahsihisodmkentstateids0

@schedule(cron_schedule="0 10 3 * *", job=implnet_job_cuahsicuahsihisodmkentstateids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisodmkentstateids0(_context):
    run_config = {}
    return run_config
