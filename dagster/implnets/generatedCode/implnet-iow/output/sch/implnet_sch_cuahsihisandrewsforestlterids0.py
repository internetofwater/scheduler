from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisandrewsforestlterids0 import implnet_job_cuahsihisandrewsforestlterids0

@schedule(cron_schedule="0 8 16 * *", job=implnet_job_cuahsihisandrewsforestlterids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisandrewsforestlterids0(_context):
    run_config = {}
    return run_config
