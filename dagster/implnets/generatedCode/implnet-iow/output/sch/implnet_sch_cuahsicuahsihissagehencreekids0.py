from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihissagehencreekids0 import implnet_job_cuahsicuahsihissagehencreekids0

@schedule(cron_schedule="0 10 4 * *", job=implnet_job_cuahsicuahsihissagehencreekids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihissagehencreekids0(_context):
    run_config = {}
    return run_config
