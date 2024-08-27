from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihishydrodataczdids0 import implnet_job_cuahsihishydrodataczdids0

@schedule(cron_schedule="0 12 17 * *", job=implnet_job_cuahsihishydrodataczdids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihishydrodataczdids0(_context):
    run_config = {}
    return run_config
