from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisnceiww2ids0 import implnet_job_cuahsihisnceiww2ids0

@schedule(cron_schedule="0 4 14 * *", job=implnet_job_cuahsihisnceiww2ids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisnceiww2ids0(_context):
    run_config = {}
    return run_config
