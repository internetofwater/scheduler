from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihistncwaterdataids0 import implnet_job_cuahsicuahsihistncwaterdataids0

@schedule(cron_schedule="0 16 1 * *", job=implnet_job_cuahsicuahsihistncwaterdataids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihistncwaterdataids0(_context):
    run_config = {}
    return run_config
