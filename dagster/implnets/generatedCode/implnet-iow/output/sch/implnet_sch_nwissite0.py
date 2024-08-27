from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwissite0 import implnet_job_nwissite0

@schedule(cron_schedule="0 16 13 * *", job=implnet_job_nwissite0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwissite0(_context):
    run_config = {}
    return run_config
