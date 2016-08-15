from __future__ import print_function
from __future__ import absolute_import

import os
import sys
import platform
from subprocess import Popen
from .util import DepsResolver
import glob
import zipfile
import traceback

# Interfaces for extensibility
class QuarkAsset:
    def __init__(self, env, asset_path, asset_args, deployment_config, spark_version="1.6.0"):
        self.env = env
        self.deployment_config = deployment_config
        self.asset_path = asset_path
        self.asset_args = asset_args
        self.spark_version = spark_version
        depsDir = self.deployment_config.get(self.env, "deps_dir")
        self.resolver = DepsResolver(depsDir, spark_version)

    def package(self):
        pass

    def deploy(self):
        pass

    def test(self):
        pass

    def validate(self):
        pass


# Currently it packages everything -- ideally should be only common
# and what the project needs. It's not necessary yet
# TODO: refactor this better
class PythonAsset(QuarkAsset):
    def package(self, packagePath):
        if type(packagePath) == list:
            packagePath = packagePath[0]
        print("packagePath",packagePath)
        zf = zipfile.ZipFile(packagePath, mode='w')
        projectsDir = self.deployment_config.get(self.env, "projects_dir")
        pythonProjectsDir = os.path.join(os.path.abspath(projectsDir),"python")
        try:
            for filename in glob.iglob(pythonProjectsDir+'/**/*.py'):
                print("Adding " + filename)
                zf.write(filename, arcname="spark-dev/"+filename.replace(pythonProjectsDir, "python"))
            for filename in glob.iglob(projectsDir+'/**/*.json'):
                print("Adding " + filename)
                zf.write(filename, arcname="spark-dev/"+filename.replace(projectsDir, ""))
            #TODO: get the actual dir
            for filename in ["bin/quark", "bin/quark_in_mesos.sh"]:
                print("Adding " + filename)
                zf.write(filename)
            for filename in glob.iglob("bin/quark_lib/*.py"):
                print("Adding " + filename)
                zf.write(filename)
            zf.write("deployment_profiles.cfg", "spark-dev/deployment_profiles.cfg")
        finally:
            zf.close()


    @staticmethod
    def expandJarsDir(jarsDirs):
        if not type(jarsDirs) == list:
            jarsDirs = [jarsDirs]
        allJars = []
        for jar_dir in jarsDirs:
            allFiles = [os.path.join(jar_dir,f) for f in os.listdir(jar_dir) if os.path.isfile(os.path.join(jar_dir,f))]
            jarFiles = filter(lambda x: x.endswith(".jar"), allFiles)
            allJars.extend(jarFiles)
        return allJars

    def getSparkSubmitArgs(self):
        depsDir =self.deployment_config.get(self.env, "deps_dir")
        sparkDir = self.resolver.dirpath("spark")
        tmpDir = self.deployment_config.get(self.env, "tmp_dir")
        eventsDir = os.path.join(tmpDir,"events")
        if not os.path.exists(eventsDir):
            os.makedirs(eventsDir)
        eventLogEnabled = self.deployment_config.get(self.env, "enable_event_logs") == "true"
        metricsConfPath = self.deployment_config.get(self.env, "metrics_conf")
        jars = self.deployment_config.get(self.env, "jars")
        jarsDirs = self.deployment_config.get(self.env, "jars_dirs").split(",")
        pyFiles = ",".join(map(lambda x: os.path.abspath(x), filter(lambda x: os.path.isfile(x),self.deployment_config.get(self.env, "py_files").split(","))))
        projectsDir = self.deployment_config.get(self.env, "projects_dir")
        pythonProjectsDir = os.path.join(os.path.abspath(projectsDir),"python")
        #pyFiles = map(lambda x: os.path.abspath(x), filter(lambda x: os.path.isfile(x),self.config.get(self.options.env, "py_files").split(","))
        master = self.deployment_config.get(self.env, "master")
        packages = self.deployment_config.get(self.env, "packages")
        mesosLibsDir = ""
        if not jars.endswith(","):
            jars += ","
        allJars = []
        for jarsDir in jarsDirs:
            allJars.extend(PythonAsset.expandJarsDir(jarsDir))
        jars += ','.join(allJars)
        if master.startswith("mesos"):
            remoteDeps = self.deployment_config.get(self.env, "remote_deps_dir")
            mesosLibsDir= self.deployment_config.get(self.env, "mesos_libs_dir")

            execList = ["--jars", jars, "--driver-class-path", jars.replace(depsDir,remoteDeps), "--master", master]
        else:
            execList = ["--jars", jars, "--driver-class-path", jars, "--master", master]
        # TODO: expose this separately in cfg
        if self.env != "local":
            execList.extend(["--deploy-mode", "cluster"])


        if pyFiles != "":
            execList.extend(["--py-files", pyFiles])
        if packages != "":
            execList.extend(["--packages", packages])
        if eventLogEnabled:
            execList.extend(["--conf", "spark.eventLog.enabled=true",
                             "--conf",
                             "spark.eventLog.dir={}".format(eventsDir),
                             "--conf",
                             "spark.metrics.conf={}".format(metricsConfPath)])

        execList.append(self.asset_path)
        execList.extend(self.asset_args)


        print(" ".join(execList))
        pythonPath = pythonProjectsDir
        if "PYTHONPATH" in os.environ:
            pythonPath += ":" + os.environ["PYTHONPATH"]
        if platform.system() == "Darwin":
            mesosLibPath = os.path.abspath(mesosLibsDir+"/libmesos.dylib")
        else:
            mesosLibPath = os.path.abspath(mesosLibsDir+"/libmesos.so")
        envDict = dict(os.environ,MESOS_NATIVE_JAVA_LIBRARY=mesosLibPath,
                       PYTHONPATH=pythonPath)
        return (execList, envDict)

    def deploy(self, args):
        depsDir = self.deployment_config.get(self.env, "deps_dir")
        sparkDir = self.resolver.dirpath("spark")
        print("spark", sparkDir)
        sparkSubmit = os.path.join(sparkDir, "bin", "spark-submit")
        args, envDict = self.getSparkSubmitArgs()
        args.insert(0,sparkSubmit)
        print(envDict)

        #print(args)
        process = Popen(args, env=envDict, shell=False)
        pid = process.pid
        print("Created process with pid {}".format(pid))
        process.wait()
        # try:
        #     import psutil
        #     import time
        #     me = os.getpid()
        #     while True:
        #         children = psutil.Process(me).children(recursive=True)
        #         for child in children:
        #             print(child)
        #         # Probably should print out the output of the command in the time being too
        #         time.sleep(10)
        # except:
        #     e = sys.exc_info()[0]
        #     print("psutil isn't loaded in your environment, not going to track children. Error: {}".format(e))
        #     print(traceback.format_exc())
        #     process.wait()





    # This code has been superseeded by the implementation in quark.py
    # I couldn't get the spark context
    def notebook(self):
        pyspark = self.resolver.dirpath("pyspark-shell")
        args, envDict = self.getSparkSubmitArgs()
        # IPYTHON_OPTS was removed in Spark 2.0.0
        print(self.spark_version.split(".")[0])
        if self.spark_version.split(".")[0] == "1":
            envDict["IPYTHON_OPTS"]="notebook --port 8889 \
--notebook-dir='{}' \
--ip='*' --no-browser".format(self.asset_path)
        else:
            envDict["PYSPARK_DRIVER_PYTHON_OPTS"]="notebook --port 8889 \
--notebook-dir='{}' \
--ip='*' --no-browser".format(self.asset_path)
        # You might be asking.. like O EM GEE, WHY all this complicated shit?
        # Apparently matplotlib is horribly broken in OSX if you're using virtualenv
        # http://matplotlib.org/faq/virtualenv_faq.html
        # The GUI frameworks that matplotlib uses to draw shit
        import platform
        print(os.environ)
        pythonExec = "python"
        if platform.system() == "Darwin":
            # I'm just going to assume you're using a virtualenv
            pythonExec = "/usr/local/bin/python2.7"
        envDict["PYSPARK_SUBMIT_ARGS"] = ' '.join(args)+" pyspark-shell" + " --master " + self.deployment_config.get(self.env, "master")
        args.insert(0, pyspark)
        print(args)
        Popen([pythonExec,"-mjupyter","notebook", "--port", "8889", "--notebook-dir=", self.asset_path], env=envDict).wait()


    def test(self):
        pass

    def validate(self):
        spark_dir = self.resolver("spark")
        sys.path.insert(1, os.path.join(spark_dir, 'python'))
        from pylint import epylint as lint
        lint.py_run(self.asset_path)
        Popen(["flake8", self.asset_path]).wait()
