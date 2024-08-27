from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisfarmrwaids0 import implnet_job_cuahsihisfarmrwaids0

@schedule(cron_schedule="0 4 7 * *", job=implnet_job_cuahsihisfarmrwaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisfarmrwaids0(_context):
    run_config = {}
    return run_config
