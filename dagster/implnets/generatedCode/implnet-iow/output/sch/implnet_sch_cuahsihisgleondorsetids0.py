from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisgleondorsetids0 import implnet_job_cuahsihisgleondorsetids0

@schedule(cron_schedule="0 6 5 * *", job=implnet_job_cuahsihisgleondorsetids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisgleondorsetids0(_context):
    run_config = {}
    return run_config
