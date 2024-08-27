from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nmwdiosenmwdiose4 import implnet_job_nmwdiosenmwdiose4

@schedule(cron_schedule="0 10 8 * *", job=implnet_job_nmwdiosenmwdiose4, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nmwdiosenmwdiose4(_context):
    run_config = {}
    return run_config
