from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihislaselvastreamdischargeids0 import implnet_job_cuahsihislaselvastreamdischargeids0

@schedule(cron_schedule="0 20 19 * *", job=implnet_job_cuahsihislaselvastreamdischargeids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihislaselvastreamdischargeids0(_context):
    run_config = {}
    return run_config
