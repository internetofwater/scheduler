from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihishydrodataczdids0 import implnet_job_cuahsicuahsihishydrodataczdids0

@schedule(cron_schedule="0 14 5 * *", job=implnet_job_cuahsicuahsihishydrodataczdids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihishydrodataczdids0(_context):
    run_config = {}
    return run_config
