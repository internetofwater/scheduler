from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihistncwaterdataids0 import implnet_job_cuahsihistncwaterdataids0

@schedule(cron_schedule="0 16 1 * *", job=implnet_job_cuahsihistncwaterdataids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihistncwaterdataids0(_context):
    run_config = {}
    return run_config
