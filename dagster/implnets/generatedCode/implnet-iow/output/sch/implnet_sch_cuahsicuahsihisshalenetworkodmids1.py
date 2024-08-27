from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisshalenetworkodmids1 import implnet_job_cuahsicuahsihisshalenetworkodmids1

@schedule(cron_schedule="0 12 1 * *", job=implnet_job_cuahsicuahsihisshalenetworkodmids1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisshalenetworkodmids1(_context):
    run_config = {}
    return run_config
