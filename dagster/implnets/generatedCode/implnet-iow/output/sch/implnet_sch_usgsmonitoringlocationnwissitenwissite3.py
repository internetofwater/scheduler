from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwissitenwissite3 import implnet_job_usgsmonitoringlocationnwissitenwissite3

@schedule(cron_schedule="0 12 13 * *", job=implnet_job_usgsmonitoringlocationnwissitenwissite3, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwissitenwissite3(_context):
    run_config = {}
    return run_config
