from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisandrewsforestlterids0 import implnet_job_cuahsicuahsihisandrewsforestlterids0

@schedule(cron_schedule="0 22 2 * *", job=implnet_job_cuahsicuahsihisandrewsforestlterids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisandrewsforestlterids0(_context):
    run_config = {}
    return run_config
