from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihiscocorahsids0 import implnet_job_cuahsihiscocorahsids0

@schedule(cron_schedule="0 6 4 * *", job=implnet_job_cuahsihiscocorahsids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihiscocorahsids0(_context):
    run_config = {}
    return run_config
