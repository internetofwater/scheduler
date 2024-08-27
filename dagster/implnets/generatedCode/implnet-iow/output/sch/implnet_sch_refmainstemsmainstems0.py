from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refmainstemsmainstems0 import implnet_job_refmainstemsmainstems0

@schedule(cron_schedule="0 20 3 * *", job=implnet_job_refmainstemsmainstems0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refmainstemsmainstems0(_context):
    run_config = {}
    return run_config
