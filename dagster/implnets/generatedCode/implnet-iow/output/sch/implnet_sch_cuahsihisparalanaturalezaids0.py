from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisparalanaturalezaids0 import implnet_job_cuahsihisparalanaturalezaids0

@schedule(cron_schedule="0 20 3 * *", job=implnet_job_cuahsihisparalanaturalezaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisparalanaturalezaids0(_context):
    run_config = {}
    return run_config
