import os
## Provided here mostly as a spec to keep things together
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

class DepsResolver:
    def __init__(self, base_dir, spark_version="1.6.0"):
        self.base_dir = base_dir
        self.spark_version = spark_version

    def dirpath(self,name):
        paths = deps_paths[self.spark_version]
        return os.path.join(self.base_dir, paths[name])
