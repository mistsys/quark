# Quark

- Quark is a minimal service that tries to make intricacies of working with Spark less crazy


## Setting up

`./bin/setup-dev-env.sh`

This creates everything inside `_dependencies` directory based on `deps` file.

`deps` file is simply a list of downloadables that are needed. They need to be the same in both server and client for easy dev

## Supported Languages

- Python 2 & 3
- Java 1.8+

Scala as libraries are fine but use JavaAPI to call them. Clojure has always been a pain in this front, so not supporting. Sparkling was a nice little step but not good enough.

## Vagrant workflow

I think development and testing should replicate within reason what the production should be.. along with partitions, failures.

Your dev environment is the most ideal this will ever run perfectly. Make sure tests are there for failures and weird data.

So, to that end, you can run `vagrant up` and by default it will run two vagrant instances - one master, one slave.

You can control that by `MESOS_SLAVES` environment variable. So to spin up 3 slaves, `MESOS_SLAVES=3 vagrant up`

## Adding environments

It is all managed by `deployment-profiles.cfg` -- your standard python based configParser with interpolation.

# Quark

## Supported tasks

`test` - No production deployments without test code

`validate` - pylint, flake8 for python and eventually PMD and FindBugs for Java

`deploy` - for actually running. Running locally is really just deploy with `--env local` which is default

`package` - Packages files in projects/{java,python} repos

`history` - Starts/Stops the history server

- Quark is not a build system -- it commands other build systems to do stuff

## Quark Commands

### Running Locally
```
bin/quark --env local --task deploy examples/python/spark_read_secor_files.py
```
If you need more parallelization with Spark set `master: local[*]` -- this will use all the cores available. I set it to 2 by default.


### Submitting to Vagrant
```
bin/quark --env vagrant-submitter --task deploy examples/python/spark_read_secor_files.py
```

### Submitting to Chronos

```
curl -L -H 'Content-Type: application/json' -X POST -d @chronos-schedules/spark-ap-stats.json http://mesos-slave-002-staging.mistsys.net:4400/scheduler/iso8601
```

#### killing jobs

```
curl -X DELETE http://mesos-slave-002-staging.mistsys.net:4400/scheduler/task/kill/spark-ap-stats
```

### Running from inside Vagrant

```
bin/quark --env vagrant --task deploy examples/python/spark_read_secor_files.py
```

### Running in Docker


```
docker run -v $PWD:/data -v $HOME/.aws:/root/.aws -it mistsys/quark --env docker --profiles-file /conf/deployment_profiles.cfg --task deploy /data/projects/python/pace_assoc_event_transformer/assoc_event_processor.py
```

### Running in Mesos/Chronos

Look at the `conf/chronos-schedules/quark-assoc-event-transformer-simple.json` file

### Documentation

```
bin/quark --task doc
```

You

# Contributing

few things in terms of workflow and contributions
1. Git hygiene —> No stupid merges. if you keep the git commit clean everyone’s happier..  `git rebase`. I will revert commits and make people do it correctly. I will revert your commit and make you do it correctly.
2. Quark shouldn’t require virtualenv setup —> only stdlib unless it grows bigger than a few files. The way I’m thinking is — Quark really should work despite your virtualenv setup.. keeping deps minimal is a good way to do that
