from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisfcelterids0 import implnet_job_cuahsicuahsihisfcelterids0

@schedule(cron_schedule="0 22 6 * *", job=implnet_job_cuahsicuahsihisfcelterids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisfcelterids0(_context):
    run_config = {}
    return run_config
