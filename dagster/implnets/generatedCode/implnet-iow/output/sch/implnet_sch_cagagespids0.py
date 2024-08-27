from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cagagespids0 import implnet_job_cagagespids0

@schedule(cron_schedule="0 4 21 * *", job=implnet_job_cagagespids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cagagespids0(_context):
    run_config = {}
    return run_config
