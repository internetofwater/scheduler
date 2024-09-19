# Monitoring workfows

## scheduler interface

### check the run interface

http://localhost:3000/runs

If there is a failure, click on the runid of the run, then you can look at the run log

## portainer status

The gleaner and nabu are run as services are prefixed with sch\_
pattern is

sch_PROJECT_step

so if it looks like something is not working, find the container starting with sch_project_step,

then go into a terminal,

you may need to use bin/sh

```shell
cd logs
ls -l
tail some log name
```
