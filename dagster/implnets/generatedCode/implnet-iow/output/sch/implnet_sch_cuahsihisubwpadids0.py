from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisubwpadids0 import implnet_job_cuahsihisubwpadids0

@schedule(cron_schedule="0 20 17 * *", job=implnet_job_cuahsihisubwpadids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisubwpadids0(_context):
    run_config = {}
    return run_config
