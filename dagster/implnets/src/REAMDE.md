# ScriptBuilder

## testing running

Once built you can ```cd``` into the output directory and run a command like:

```bash 
 dagit -h ghost.lan -w workspace.yaml
 ```

## About

Script to build the dagster files based on templates and a Gleaner config file.

```
sed -i  's|0 15 \* \* \*|0 1 \* \* \*|g' implnet_sch_aquadocs.py
```

We do not do assets or sensors at this time so they are not involved here.
Thy would be their oen directories in the output and then need to be 
represented in the repository.py file. 

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