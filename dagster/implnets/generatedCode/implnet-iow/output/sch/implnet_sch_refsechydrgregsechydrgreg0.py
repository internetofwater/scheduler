from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refsechydrgregsechydrgreg0 import implnet_job_refsechydrgregsechydrgreg0

@schedule(cron_schedule="0 8 3 * *", job=implnet_job_refsechydrgregsechydrgreg0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refsechydrgregsechydrgreg0(_context):
    run_config = {}
    return run_config
