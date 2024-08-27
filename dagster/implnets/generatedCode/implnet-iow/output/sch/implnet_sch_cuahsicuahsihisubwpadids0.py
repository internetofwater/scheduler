from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisubwpadids0 import implnet_job_cuahsicuahsihisubwpadids0

@schedule(cron_schedule="0 14 3 * *", job=implnet_job_cuahsicuahsihisubwpadids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisubwpadids0(_context):
    run_config = {}
    return run_config
