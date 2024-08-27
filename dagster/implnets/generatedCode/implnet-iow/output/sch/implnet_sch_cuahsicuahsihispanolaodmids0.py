from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihispanolaodmids0 import implnet_job_cuahsicuahsihispanolaodmids0

@schedule(cron_schedule="0 10 6 * *", job=implnet_job_cuahsicuahsihispanolaodmids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihispanolaodmids0(_context):
    run_config = {}
    return run_config
