from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisredbuttecreekgamutids0 import implnet_job_cuahsicuahsihisredbuttecreekgamutids0

@schedule(cron_schedule="0 14 6 * *", job=implnet_job_cuahsicuahsihisredbuttecreekgamutids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisredbuttecreekgamutids0(_context):
    run_config = {}
    return run_config
