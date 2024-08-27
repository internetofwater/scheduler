from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihislaselvastreamdischargeids0 import implnet_job_cuahsicuahsihislaselvastreamdischargeids0

@schedule(cron_schedule="0 12 5 * *", job=implnet_job_cuahsicuahsihislaselvastreamdischargeids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihislaselvastreamdischargeids0(_context):
    run_config = {}
    return run_config
