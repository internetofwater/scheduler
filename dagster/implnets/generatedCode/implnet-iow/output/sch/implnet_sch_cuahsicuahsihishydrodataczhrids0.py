from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihishydrodataczhrids0 import implnet_job_cuahsicuahsihishydrodataczhrids0

@schedule(cron_schedule="0 2 4 * *", job=implnet_job_cuahsicuahsihishydrodataczhrids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihishydrodataczhrids0(_context):
    run_config = {}
    return run_config
