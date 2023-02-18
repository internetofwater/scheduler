# Dagster


## About

The following is a description of the steps and requirements for
building and deploying the docker based workflow implemented in 
dagster.

### Broad overview

The image following provides a broad overview of the elements that 
are loaded in to the Docker orchestration environment.  This is a very 
basic view and doesn't present any scaling or fail over elements.  

The key elements are:

* sources to configuration and then the creation of the archive files that are loaded and used
to load into the Gleaner and Nabu tools
* The Dagster set which loads three containers to support workflow operations
* The Gleaner Architecture images which loads three or more containers to support 
  * s3 object storage
  * graph database (triplestore)
  * headless chrome for page rendering to support dynamically inserted JSON-LD
  * any other support packages like text, semantic or spatial indexes
* The GleanerIO tools which loads two containers (Gleaner and Nabu) that are run 
and removed by the Dagster workflow

![upper level](../docs/images/gleanerDagster.svg)



### The asset file flow in more detail:

* Creation of template files for the various operations, jobs and 
schedules
* Creation of the archive files that hold the configuration for the 
jobs run 
* Environment file for the values needed by the operations

![flow](../docs/images/flow.svg)


### Template files

The template files define the Dagster Ops, Jobs and Schedules.  From these
and a GleanerIO config file a set of Python scripts for Dagster are created in
the output directory. 

These only need to be changed or used to regenerate if you wish to alter the 
execution graph (ie, the ops, jobs and schedules) or change the config file.
In the later case only a regeneration needs to be done.

There are then Docker build scripts to build out new containers.  

See:  [template](./dagster-docker/src/implnet-example/templates)

### Archive files

The archive files need to be compress tar files with the Gleaner
or Nabu configs and other required files like the schema context. 

The archive files are defined in:

```yaml
        ARCHIVE_FILE = os.environ.get('GLEANERIO_GLEANER_ARCHIVE_OBJECT')
        ARCHIVE_PATH = os.environ.get('GLEANERIO_GLEANER_ARCHIVE_PATH')

        ARCHIVE_FILE = os.environ.get('GLEANERIO_NABU_ARCHIVE_OBJECT')
        ARCHIVE_PATH = os.environ.get('GLEANERIO_NABU_ARCHIVE_PATH')
```
The required config file names are expressed in the CMD line:

```yaml
        CMD = ["--cfg", "/gleaner/gleanerconfig.yaml", "--source", source]

        CMD = ["--cfg", "/nabu/nabuconfig.yaml", "prefix", "summoned/" + source]
```

So:  gleanerconfig.yaml  and nabuconfig.yaml are the required configuration names.

> NOTE: At present only yaml is supported, JSON support is a simple addition 
> once the system is tested and working OK with the yaml files. 

The contents will look something like 

```bash
❯ tar -ztf GleanerCfg.tgz
./gleanerconfig.yaml
./jsonldcontext.json
❯ tar -ztf NabuCfg.tgz
./nabuconfig.yaml
./jsonldcontext.json
```

These files need to be in the bucket prefix defined by: 

```yaml
GLEANERIO_GLEANER_ARCHIVE_OBJECT=scheduler/configs/GleanerCfg.tgz
GLEANERIO_NABU_ARCHIVE_OBJECT=scheduler/configs/NabuCfg.tgz
```

### Environment files

``` bash
export PORTAINER_URL=http://example.org:9000/api/endpoints/2/docker/
export PORTAINER_KEY=KEY

export GLEANERIO_GLEANER_IMAGE=fils/gleaner:v3.0.11-development-df
export GLEANERIO_GLEANER_ARCHIVE_OBJECT=scheduler/configs/testGleanerCfg.tgz
export GLEANERIO_GLEANER_ARCHIVE_PATH=/gleaner/

export GLEANERIO_NABU_IMAGE=fils/nabu:2.0.4-developement
export GLEANERIO_NABU_ARCHIVE_OBJECT=scheduler/configs/testNabuCfg.tgz
export GLEANERIO_NABU_ARCHIVE_PATH=/nabu/

export GLEANERIO_LOG_PREFIX=scheduler/logs/

export GLEANER_MINIO_URL=192.168.202.114
export GLEANER_MINIO_PORT=49153
export GLEANER_MINIO_SSL=False
export GLEANER_MINIO_SECRET=SECRET
export GLEANER_MINIO_KEY=KEY
export GLEANER_MINIO_BUCKET=gleaner.test

```

## Setup


![orchestration](../docs/images/orchestrationInit.svg)

## Docker API sequence

![sequence](../docs/images/sequence.svg)


## Appendix

### Portainer API setup

You will need to setup Portainer to allow for an API call.  To do this look 
at the documentation for [Accessing the Portainer API](https://docs.portainer.io/api/access)

## Notes

Single file testing run

```bash
 dagit -h ghost.lan -f test1.py
```

* Don't forget to set the DAGSTER_HOME dir like in 

```bash
 export DAGSTER_HOME=/home/fils/src/Projects/gleaner.io/scheduler/python/dagster
```

```
dagster-daemon run
```

Run from directory where workspace.yaml is.
```
dagit --host 192.168.202.159
```


## Cron Notes

A useful on-line tool:  [https://crontab.cronhub.io/](https://crontab.cronhub.io/)

```
0 3 * * *   is at 3 AM each day

0 3,5 * * * at 3 and 5 am each day

0 3 * * 0  at 3 am on Sunday

0 3 5 * *  At 03:00 AM, on day 5 of the month

0 3 5,19 * * At 03:00 AM, on day 5 and 19 of the month

0 3 1/4 * * At 03:00 AM, every 4 days
```


## Indexing Approaches

The following approaches

* Divide up the sources by sitemap and sitegraph
* Also divide by production and queue sources

The above will result in at most 4 initial sets.

We can then use the docker approach

```
./gleanerDocker.sh -cfg /gleaner/wd/rundir/oih_queue.yaml  --source cioosatlantic
```

to run indexes on specific sources in these configuration files.  

## References

* [Simple Dagster example](https://bakerwho.github.io/posts/datascience/Deployable-Dagster-MVP/)
