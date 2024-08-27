from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisgleondorsetids0 import implnet_job_cuahsicuahsihisgleondorsetids0

@schedule(cron_schedule="0 6 5 * *", job=implnet_job_cuahsicuahsihisgleondorsetids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisgleondorsetids0(_context):
    run_config = {}
    return run_config
