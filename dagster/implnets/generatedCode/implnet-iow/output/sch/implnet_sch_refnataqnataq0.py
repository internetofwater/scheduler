from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refnataqnataq0 import implnet_job_refnataqnataq0

@schedule(cron_schedule="0 18 4 * *", job=implnet_job_refnataqnataq0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refnataqnataq0(_context):
    run_config = {}
    return run_config
