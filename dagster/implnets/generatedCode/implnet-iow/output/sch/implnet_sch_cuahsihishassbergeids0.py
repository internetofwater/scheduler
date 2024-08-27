from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihishassbergeids0 import implnet_job_cuahsihishassbergeids0

@schedule(cron_schedule="0 12 3 * *", job=implnet_job_cuahsihishassbergeids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihishassbergeids0(_context):
    run_config = {}
    return run_config
