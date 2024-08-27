from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisshalenetworkodmids0 import implnet_job_cuahsicuahsihisshalenetworkodmids0

@schedule(cron_schedule="0 4 1 * *", job=implnet_job_cuahsicuahsihisshalenetworkodmids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisshalenetworkodmids0(_context):
    run_config = {}
    return run_config
