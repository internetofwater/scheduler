from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nmwdiosenmwdiose2 import implnet_job_nmwdiosenmwdiose2

@schedule(cron_schedule="0 8 8 * *", job=implnet_job_nmwdiosenmwdiose2, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nmwdiosenmwdiose2(_context):
    run_config = {}
    return run_config
