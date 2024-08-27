from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisparalanaturalezaids0 import implnet_job_cuahsicuahsihisparalanaturalezaids0

@schedule(cron_schedule="0 20 3 * *", job=implnet_job_cuahsicuahsihisparalanaturalezaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisparalanaturalezaids0(_context):
    run_config = {}
    return run_config
