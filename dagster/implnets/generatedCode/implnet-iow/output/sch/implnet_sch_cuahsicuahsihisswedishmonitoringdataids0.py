from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisswedishmonitoringdataids0 import implnet_job_cuahsicuahsihisswedishmonitoringdataids0

@schedule(cron_schedule="0 0 4 * *", job=implnet_job_cuahsicuahsihisswedishmonitoringdataids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisswedishmonitoringdataids0(_context):
    run_config = {}
    return run_config
