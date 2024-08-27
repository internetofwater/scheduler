from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihistuolumnemdwids0 import implnet_job_cuahsicuahsihistuolumnemdwids0

@schedule(cron_schedule="0 18 2 * *", job=implnet_job_cuahsicuahsihistuolumnemdwids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihistuolumnemdwids0(_context):
    run_config = {}
    return run_config
