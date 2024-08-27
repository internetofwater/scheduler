from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisfcelterids0 import implnet_job_cuahsihisfcelterids0

@schedule(cron_schedule="0 12 18 * *", job=implnet_job_cuahsihisfcelterids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisfcelterids0(_context):
    run_config = {}
    return run_config
