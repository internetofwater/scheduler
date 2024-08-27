from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihishassbergeids0 import implnet_job_cuahsicuahsihishassbergeids0

@schedule(cron_schedule="0 12 3 * *", job=implnet_job_cuahsicuahsihishassbergeids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihishassbergeids0(_context):
    run_config = {}
    return run_config
