from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihismmaatacamaids0 import implnet_job_cuahsicuahsihismmaatacamaids0

@schedule(cron_schedule="0 6 3 * *", job=implnet_job_cuahsicuahsihismmaatacamaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihismmaatacamaids0(_context):
    run_config = {}
    return run_config
