from dagster import job

from ops.implnet_ops_wadewade41 import harvest_wadewade41

@job
def implnet_job_wadewade41():
    harvest_wadewade41()
