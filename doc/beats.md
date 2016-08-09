# Beats

## Spec


Current Version: 1

Beats is inspired by similar concept for scheduling jobs in Celery, but this is for Chronos and Spark. This also allows us to decouple the runtime environment for periodic jobs from the definition of the jobs themselves.


```
{
    "version": "<version of schema>",
    "name": "<name of the job>",
    "frequency": "<n> <time unit>",
    "start_time": "ISO8601 formatted start time",
    "owner": "<email address>",
    "command": "<command/script to run>",
    "schedule_on": ["chronos", "lambda"],
    "vars": {... map ...}
}
```

## Required Fields ##

### Frequency ###

This requires the full `1 minute` or similar time unit. Supported time units are `minute(s)` `hour(s)` `day(s)` `week(s)` `month(s)` and `year(s)`

Second level frequency is out of scope. At that point, you might as well write a small program.

### Owner ###

Email address(es) of who needs to get notified.

### Command ###

What command to run


## Optional Fields ##

### Name ###

Quark enforces that the name should start with the directory name of the project

## Start Time ##

If you want it to start.. this will lead to a warning if it's in the past. The idea is to give a frequency and it'll be scheduled at the top of the hour. Start time ought to be given if you want it to run in a different schedule like the 22:57 minute mark or if you want it to start later.

## Deployment

You can run `gen_beats` task in quark to validate and create a common `all_beats.json` file that's composed of all the beats in directories. This is uploaded to s3 and used to schedule the job in Chronos.
