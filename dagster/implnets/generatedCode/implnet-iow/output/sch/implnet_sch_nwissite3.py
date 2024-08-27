from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwissite3 import implnet_job_nwissite3

@schedule(cron_schedule="0 0 6 * *", job=implnet_job_nwissite3, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwissite3(_context):
    run_config = {}
    return run_config
