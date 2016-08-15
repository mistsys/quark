from __future__ import print_function
import os
import sys
## Provided here mostly as a spec to keep things together
# After its open source, it may be just part of assets
# or can be overridden depending on organization's needs
default_args_map = {
    'master': '',
    'jars': '',
    'deps_dir': '',
    'remote_deps_dir': ''
}

deps_paths = {
    "2.0.0": {
        "spark":"spark-2.0.0-bin-hadoop2.7",
        "java_libs": "libs",
        "kafka": "kafka_2.10-0.10.0.0",
        "mesos": "mesos-0.25.0",
        "pyspark-shell": "spark-2.0.0-bin-hadoop2.7/python/pyspark/shell.py",
        "py4j": "spark-2.0.0-bin-hadoop2.7/python/lib/py4j-0.10.1-src.zip"
    },
    "1.6.0":{
        "spark":"spark-1.6.0-bin-hadoop2.6",
        "java_libs": "libs",
        "kafka": "kafka_2.10-0.8.2.2",
        "mesos": "mesos-0.25.0",
        "pyspark-shell": "spark-1.6.0-bin-hadoop2.6/python/pyspark/shell.py",
        "py4j": "spark-1.6.0-bin-hadoop2.6/python/lib/py4j-0.9-src.zip"
    }
}

spark_downloads = {
    "1.6.0": {
        "spark": "http://d3kbcqa49mib13.cloudfront.net/spark-1.6.0-bin-hadoop2.6.tgz",
        "deps": [
            "http://central.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.10/1.6.0-M1/spark-cassandra-connector_2.10-1.6.0-M1.jar",
            "http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka_2.10/1.6.0/spark-streaming-kafka_2.10-1.6.0.jar",
            "http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.0/hadoop-aws-2.7.0.jar",
            "http://mirror.reverse.net/pub/apache/kafka/0.10.0.0/kafka_2.10-0.10.0.0.tgz",
            "http://archive.apache.org/dist/mesos/0.25.0/mesos-0.25.0.tar.gz"
        ]
    },
    "2.0.0": {
        "spark": "http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.7.tgz",
        "deps": [
            "http://mirror.reverse.net/pub/apache/kafka/0.10.0.0/kafka_2.10-0.10.0.0.tgz",
            "http://archive.apache.org/dist/mesos/1.0.0/mesos-1.0.0.tar.gz"
        ]

    },

}

basic_config_tmpl = {
    "1.6.0":"""
[DEFAULT]
deployment_environment: local
deps_dir: %(PWD)s/_dependencies
jars: %(deps_dir)s/libs/aws-java-sdk-1.7.4.jar,%(deps_dir)s/libs/hadoop-aws-2.7.0.jar,%(deps_dir)s/libs/spark-streaming-kafka_2.10-1.6.0.jar
# Includes all the jars here and more
jars_dirs: %(deps_dir)s/kafka_2.10-0.8.2.2/libs
master: local[2]
# Override this if you want to use a different path
# If you brew installed it, it'll be in /usr/local/Cellar/mesos/0.25.0
mesos_libs_dir: %(deps_dir)s/mesos-0.25.0/build/src/.libs
tmp_dir: _tmp
history_port: 18080
enable_event_logs: true
metrics_conf: conf/metrics.properties
py_files:
packages:
projects_dir: projects
docs_dir: doc

[local]
master: local[4]
    """,

    "2.0.0": """
[DEFAULT]
deployment_environment: local
deps_dir: %(PWD)s/_dependencies
jars:
# Includes all the jars here and more
jars_dirs: %(deps_dir)s/kafka_2.10-0.8.2.2/libs
master: local[2]
# Override this if you want to use a different path
# If you brew installed it, it'll be in /usr/local/Cellar/mesos/0.25.0
mesos_libs_dir: %(deps_dir)s/mesos-0.25.0/build/src/.libs
tmp_dir: _tmp
history_port: 18080
enable_event_logs: true
metrics_conf: conf/metrics.properties
py_files:
packages:
projects_dir: projects
docs_dir: doc

[local]
master: local[4]

"""
}

class DepsResolver:
    def __init__(self, base_dir, spark_version="1.6.0"):
        self.base_dir = base_dir
        self.spark_version = spark_version

    def dirpath(self,name):
        paths = deps_paths[self.spark_version]
        return os.path.join(self.base_dir, paths[name])


    def write_deps_file(self, pth=None):
        deps_list = []
        dls = spark_downloads[self.spark_version]
        deps_list.append(dls["spark"])
        deps_list.extend(dls["deps"])
        cwd = os.getcwd()
        if pth == None:
            pth = os.path.join(cwd, "deps")
        with open(pth, "w") as f:
            for dl in deps_list:
                f.write(dl+"\n")
        print("Wrote {} dependencies to {}".format(len(deps_list), pth))


    def write_init_config(self, pth=None):
        tmpl = basic_config_tmpl[self.spark_version]
        cwd = os.getcwd()
        if pth == None:
            pth = os.path.join(cwd, "deployment_profiles.cfg")
        if os.path.exists(pth):
            print("Path {} already exists, aborting.".format(pth))
            sys.exit(1)
        with open(pth, "w") as f:
            f.write(tmpl)
        print("Wrote config to", pth)

    def download(self, name):
        pass
