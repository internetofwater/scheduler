from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihistuolumnemdwids0 import implnet_job_cuahsihistuolumnemdwids0

@schedule(cron_schedule="0 12 11 * *", job=implnet_job_cuahsihistuolumnemdwids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihistuolumnemdwids0(_context):
    run_config = {}
    return run_config
