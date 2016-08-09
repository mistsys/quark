# Introduction to Quark

Quark is just a simple wrapper script to allow configuring, packaging and deployment of spark projects. The goal is to help with the entire lifecycle of a Spark based data pipeline project, without getting in the way. Quark is built around the same philosophy for tools as a lot of other Mist Systems' tools - like logsearcher, mist_metrics, kafkaclient, etc

- Auto discover the *right* thing to do based on the *environment*
- Specific tasks for special work
- Encapsulate the complexity of dealing with complex systems like Kafka, Java dependencies, etc
- Minimal requirements and maintenance footprint

## Quark Philosophy

### Portability

Quark should stick with python stdlibs. Unless it is completely unavoidable, do not use a package that requires environment specific setup. This ensures that the library can be used anywhere and with anybody's virtual environment setup. This ensures that the dependencies with the tool itself does not conflict with the code. Python is a versatile enough language that I don't see a necessity to diverge from this philosophy.

### Maintainability

Anyone with little python skill should be able to read the code and maintain quark. When in doubt about styles, follow the [Google Style Guide](https://google.github.io/styleguide/pyguide.html).

### Separation of Concerns

Quark should not know or care about the specific use case for your particular app. If you find you're adding features to Quark to support your particular use case that cannot be accomodated.


## Setting up

Quark leaves the environment setup to well, systems that do environment setup. To that end a bash script for local setup is adequate. All you need is a deps file with urls and run `bin/setup_dev_env` and it will create a `_dependencies` directory with all the requisite files included.

## Deployment Profiles

The fundamental way to manage quark is by creating a `deployment_profile.cfg` file or supplying a file in the format to `--profiles-file` argument. We'll talk further about the setup and what you can do with profile configurations.

### Default

There are some sane defaults that I recommend putting in the top of the file

```
deps_dir: _dependencies
jars: %(deps_dir)s/libs/aws-java-sdk-1.7.4.jar,%(deps_dir)s/libs/hadoop-aws-2.7.0.jar,%(deps_dir)s/libs/spark-streaming-kafka_2.10-1.6.0.jar
# Includes all the jars here and more
jars_dirs: %(deps_dir)s/kafka_2.10-0.8.2.2/libs
master: local[2]
# Override this if you want to use a different path
# If you brew installed it, it'll be in /usr/local/Cellar/mesos/0.25.0
mesos_libs_dir: %(deps_dir)/mesos-0.25.0/build/src/.libs
tmp_dir: _tmp
# Port for viewing eventLog history
history_port: 18080
enable_event_logs: true
# Log spark metrics
metrics_conf: conf/metrics.properties
# Additional Python files to deploy
py_files:
packages:
projects_dir: projects
docs_dir: doc
```

Quark prioritizes explicitly defined jars over spark's `--package` downloads. The intention here is that if the user knows what they're doing, they will explictly override the functions.
