from __future__ import print_function, division, absolute_import  # We require Python 2.6 or later
import json
import os
import glob
from .assets import PythonAsset
from string import Template
from subprocess import Popen
from .util import DepsResolver
from .beats import Beat



class Quark:
    def __init__(self, config, options):
        # Config: operations on the environment
        self.config = config
        # Options, how your application was invoked aka, arguments
        self.options = options
        projectsDir = self.config.get(self.options.env, "projects_dir")
        schemasDir = os.path.join(projectsDir, "schemas")
        depsDir = self.config.get(self.options.env, "deps_dir")
        self.resolver = DepsResolver(depsDir, self.options.spark_version)
        self.beats = Beat(file(os.path.join(schemasDir, "beats.schema.json")).read())

    def get_asset_type(self,asset_path):
        if asset_path == "python" or asset_path.endswith(".py"):
            return PythonAsset(self.options.env, asset_path, self.options.args, self.config, spark_version=self.options.spark_version)
        else:
            raise Exception("Unsupported asset type" + asset_path)




    def invoke_task(self,name, *args):
        os.environ['ENV'] = self.options.profile
        os.environ['APPLICATION_CONFIG'] = self.get_config_for_profile(self.options.profile)
        os.environ['JVM_OPTS'] = "-Xms512m -Xmx1024m"
        if args == (None,):
            getattr(self,name)()
        else:
            getattr(self,name)(*args)

    def package(self, asset_path, package_path):
        asset = self.get_asset_type(asset_path)
        asset.package(package_path)

    def deploy(self, asset_path, *args):
        asset = self.get_asset_type(asset_path)
        asset.deploy(args)

    def validate(self, asset_path):
        asset = self.get_asset_type(asset_path)
        asset.validate()

    #def notebook(self, project=None):
        # It will be the "notebooks" dir in your CWD... easy enough to change if you want to
        #notebooksDir = "notebooks"
        #if project != None:
        #    notebooksDir = os.path.join(notebooksDir,project)
        #asset = PythonAsset(self.options.env, notebooksDir, self.options.args,self.config)
        #asset.notebook()

    def gen_beats(self, out_file='all_beats.json'):
        projectsDir = os.path.abspath(self.config.get(self.options.env, "projects_dir"))
        excluded_projects = ["python/common", "python/test", "python/batch_transformer"]
        pythonProjectsDir = os.path.join(os.path.abspath(projectsDir),"python")
        is_excluded_dir = lambda x: any(x.endswith(f) for f in excluded_projects)
        dirs_included = [dirPath  for dirPath in glob.iglob(pythonProjectsDir + '**/**') if not is_excluded_dir(dirPath)]
        all_beats = []
        for dirPath in dirs_included:
            print("dir: " + dirPath)
            beatsFile = os.path.join(dirPath, "beats.json")
            if not os.path.isfile(beatsFile):
                raise Exception("Beats file not found in " + dirPath)
            with open(beatsFile) as f:
                beats = json.loads(f.read())
                for beat in beats:
                    #print(beat)
                    if self.beats.is_valid(beat):
                        print("beat: {}".format(beat))
                        command = beat["command"]
                        d = dirPath.replace(projectsDir, "")
                        print(d)
                        command = d +  command
                        if command.startswith("/"): command=command[1:]
                        beat[u"command"] = command
                        all_beats.append(beat)
                print(all_beats)
                    #print("beat:"+ self.beats.load(beat))
        json_text = json.dumps(all_beats, indent=4, sort_keys=True)
        with open(out_file, 'w') as f:
            f.write(json_text)
        return json_text

    def pyspark(self, include_packages=True):
        projectsDir = self.config.get(self.options.env, "projects_dir")
        pythonProjectsDir = os.path.join(os.path.abspath(projectsDir),"python")
        depsDir = self.config.get(self.options.env, "deps_dir")
        sparkDir = self.resolver.dirpath("spark")
        pyspark = os.path.join(sparkDir, "bin", "pyspark")
        asset = PythonAsset(self.options.env, pythonProjectsDir, self.options.args,self.config, spark_version=self.options.spark_version)
        args, envDict = asset.getSparkSubmitArgs()
        #envDict["PYSPARK_SUBMIT_ARGS"] = ' '.join(args)+" pyspark-shell"
        envDict["IPYTHON"] = "1"
        packages = self.config.get(self.options.env, "packages")
        extraDict = self._get_python_env()
        envDict.update(extraDict)
        #args.insert(0, pysparkShell)
        print(pyspark)
        if include_packages:
            Popen([pyspark, "--packages", packages], env=envDict).wait()
        else:
            Popen([pyspark], env=envDict).wait()

    def notebook(self, project=None, include_packages=True):
        projectsDir = self.config.get(self.options.env, "projects_dir")
        pythonProjectsDir = os.path.join(os.path.abspath(projectsDir),"python")
        depsDir = self.config.get(self.options.env, "deps_dir")
        sparkDir = self.resolver.dirpath("spark")
        pyspark = os.path.join(sparkDir, "bin", "pyspark")
        notebooksDir = "notebooks"
        if project != None:
            notebooksDir = os.path.join(notebooksDir,project)
        #pysparkShell = get_dirpath(depsDir, "pyspark-shell")
        asset = PythonAsset(self.options.env, pythonProjectsDir, self.options.args,self.config,spark_version=self.options.spark_version)
        args, envDict = asset.getSparkSubmitArgs()
        #envDict["PYSPARK_SUBMIT_ARGS"] = ' '.join(args)+" pyspark-shell"

        packages = self.config.get(self.options.env, "packages")
        extraDict = self._get_python_env()
        envDict.update(extraDict)
        # You might be asking.. like O EM GEE, WHY all this complicated shit?
        # Apparently matplotlib is horribly broken in OSX if you're using virtualenv
        # http://matplotlib.org/faq/virtualenv_faq.html
        # The GUI frameworks that matplotlib use to draw shit
        import platform
        pythonExec = "python"
        if platform.system() == "Darwin":
            # I'm just going to assume you're using a virtualenv
            pythonExec = "/usr/local/bin/python2.7"

        if self.options.spark_version.split(".")[0] == "1":
            envDict["IPYTHON"] = "1"
            envDict["IPYTHON_OPTS"] = "notebook --notebook-dir=" + notebooksDir
            envDict["PYSPARK_PYTHON"] = pythonExec
        else:
            # You need IPython for this in the virtualenv
            envDict["PYSPARK_DRIVER_PYTHON_OPTS"] = "notebook --notebook-dir=" + notebooksDir
            envDict["PYSPARK_DRIVER_PYTHON"] = "ipython"


        ## TODO: refactor later
        jars = self.config.get(self.options.env, "jars")
        jarsDirs = self.config.get(self.options.env, "jars_dirs").split(",")
        allJars = []
        for jarsDir in jarsDirs:
            allJars.extend(PythonAsset.expandJarsDir(jarsDir))
        jars += ','.join(allJars)
        ## TODO: refactor later

        #args.insert(0, pysparkShell)
        print(pyspark)
        if include_packages:
            Popen([pyspark,"--packages", packages, "--jars", jars], env=envDict).wait()
        else:
            Popen([pyspark, "--jars", jars], env=envDict).wait()


    def test(self):
        projectsDir = self.config.get(self.options.env, "projects_dir")
        pythonProjectsDir = os.path.join(os.path.abspath(projectsDir),"python")
        envDict = self._get_python_env()
        #print(envDict)
        Popen(["python", "-m", "unittest", "discover", "-s", pythonProjectsDir],env=envDict).wait()


    def _get_python_env(self):
        projectsDir = self.config.get(self.options.env, "projects_dir")
        pythonProjectsDir = os.path.join(os.path.abspath(projectsDir),"python")
        sparkDir = self.resolver.dirpath("spark")
        py4jFile = self.resolver.dirpath("py4j")

        ### Ahh, this is different from SPARK_SUBMIT because well  SPARK_SUBMIT_ARGS wants jars
        # to be separated by commas and ipython doesn't handle that. SPARK_CLASSPATH works however
        # There's additional caveat with spark packages
        jars = self.config.get(self.options.env, "jars")
        jarsDirs = self.config.get(self.options.env, "jars_dirs").split(",")
        pythonPaths = []
        jars = jars.replace(",", ":")
        if not jars.endswith(":"):
            jars += ":"
        allJars = []
        for jarsDir in jarsDirs:
            allJars.extend(PythonAsset.expandJarsDir(jarsDir))
        jars += ':'.join(allJars)
        print("SPARK_HOME={}".format(sparkDir))
        print("SPARK_CLASSPATH={}".format(jars))
        #TODO full path for projects python
        pythonPaths = [sparkDir+"/python", pythonProjectsDir]
        pythonPaths.append(py4jFile)
        pyFiles = map(lambda x: os.path.abspath(x), filter(lambda x: os.path.isfile(x),self.config.get(self.options.env, "py_files").split(",")))
        pythonPaths.extend(pyFiles)
        pythonPath = ":".join(pythonPaths)
        if "PYTHONPATH" in os.environ:
            pythonPath += ":" + os.environ["PYTHONPATH"]
        return dict(os.environ, PYTHONPATH=pythonPath, SPARK_HOME=sparkDir, SPARK_JAVA_OPTS=os.getenv("SPARK_JAVA_OPTS", "-Dspark.driver.extraClassPath={}".format(jars)))


    def repl(self,language):
        if language == "python":
            self.pyspark(include_packages=False)
        else:
            print("Language " + language + "is not supported")


    def get_config_for_profile(self, profile):
        projectsDir = self.config.get(self.options.env, "projects_dir")
        configDir = os.path.join(os.path.abspath(projectsDir),"config")
        return open(os.path.join(configDir, "config_{}.json".format(profile))).read()


    def env(self, language):
        depsDir =os.path.abspath(self.config.get(self.options.env, "deps_dir"))
        sparkDir = self.resolver.dirpath("spark")
        py4jFile = self.resolver.dirpath("py4j")
        jars = self.config.get(self.options.env, "jars")
        jarsDirs = self.config.get(self.options.env, "jars_dirs").split(",")
        if not jars.endswith(","):
            jars += ","
        allJars = []
        for jarsDir in jarsDirs:
            allJars.extend(PythonAsset.expandJarsDir(jarsDir))
        jars += ','.join(allJars)
        print("SPARK_HOME={}".format(sparkDir))
        print("SPARK_SUBMIT_OPTIONS=--jars {}".format(jars))
        #TODO full path for projects python
        pythonPaths = [sparkDir+"/python", "projects/python"]
        if language == "python":
            pythonPaths.append(py4jFile)
            pyFiles = map(lambda x: os.path.abspath(x), filter(lambda x: os.path.isfile(x),self.config.get(self.options.env, "py_files").split(",")))
            pythonPaths.extend(pyFiles)
            print("PYTHONPATH={}:$PYTHONPATH".format(":".join(pythonPaths)))

    def doc(self, action="generate"):
        docsDir =os.path.abspath(self.config.get(self.options.env, "docs_dir"))
        if action == "generate":
            projectsDir = self.config.get(self.options.env, "projects_dir")
            pythonProjectsDir = os.path.join(os.path.abspath(projectsDir),"python")
            Popen(["sphinx-apidoc", "-F", "-o", docsDir, pythonProjectsDir]).wait()
            Popen(["make", "html"], cwd=docsDir).wait()
        elif action == "new":
            # Defaults to markdown
            name = raw_input("Name of file: ")
            if name == "":
                return
            sanitized_name = name.replace(" ", "_").lower()
            filepath = os.path.abspath(os.path.join(docsDir, sanitized_name +".md"))
            if os.path.exists(filepath):
                response = raw_input("Path {} already exists. Overwrite? [N]".format(sanitized_name))
                if response.strip() == "": response == "no"
                if response.lower().startswith("n"):
                    return
                elif response.lower().startswith("y"):
                    # continue
                    pass
                else:
                    print("unknown response {}", response)
                    return

            with open(filepath, 'w') as f:
                f.write("# {}".format(name.capitalize()))

            # Todo: update toctree too
            Popen([os.environ.get("EDITOR", "vim"), filepath]).wait()
        elif action == "edit":
            rst_files =glob.glob(os.path.join(docsDir,"*.rst"))
            md_files = glob.glob(os.path.join(docsDir,"*.md"))
            files = rst_files + md_files
            for i, name in enumerate(files):
                print("{}:{}".format(i, name))
            file_index = raw_input("Pick the document to edit: ")
            if not file_index.isdigit():
                print("needs to be a number.")
                return
            filepath = files[int(file_index)]
            Popen([os.environ.get("EDITOR", "vim"), filepath]).wait()


    def history(self,action):
        depsDir =self.config.get(self.options.env, "deps_dir")
        sparkDir = self.resolver.dirpath("spark")
        tmpDir = self.config.get(self.options.env, "tmp_dir")
        eventsDir = os.path.join(tmpDir,"events")
        historyPort = self.config.get(self.options.env, "history_port")
        sparkHistoryServer = os.path.join(sparkDir, "sbin", "{}-history-server.sh".format(action))
        execList = [sparkHistoryServer]
        historyOpts="-Dspark.history.fs.logDirectory={} -Dspark.history.ui.port={}".format(eventsDir, historyPort)
        print(execList, historyOpts)
        Popen(execList, env=dict(os.environ, SPARK_HISTORY_OPTS=historyOpts)).wait()

    def metrics(self):
        tmpDir = self.config.get(self.options.env, "tmp_dir")
        metricsDir = os.path.join(tmpDir,"metrics")
        filein = open("html/index.html.tmpl", "r")
        #d = {"path": os.path.join(metricsDir, "local-1458287895609.driver.PythonStreamingKafkaWordCount.StreamingMetrics.streaming.retainedCompletedBatches.csv"), "metric": "retainedCompletedBatches"}
        metricsFilesList = [os.path.join(metricsDir,f) for f in os.listdir(metricsDir) if os.path.isfile(os.path.join(metricsDir,f))]
        csvList = filter(lambda x: x.endswith(".csv"), metricsFilesList)
        filesList = []
        for l in csvList:
            items = os.path.split(l)[-1].split(".")
            metric = items[-2]  # last Item
            application = items[0]  # First item
            system = items[1]  # Driver/Executor, etc
            component = items[-3]  # memory in local-1458287895609.driver.BlockManager.memory.maxMem_MB.csv
            subcomponent = '.'.join(items[2:-3])  # Should be PythonStreamingKafkaWordCount.StreamingMetrics in local-1458287895609.driver.PythonStreamingKafkaWordCount.StreamingMetrics.streaming.lastCompletedBatch_processingDelay.csv

            if metric not in ["messageProcessingTime"]:
                filesList.append({"path": l,
                                  "type": "plot",
                                  "subcomponent": subcomponent,
                                  "system": system,
                                  "application": application,
                                  "component": component,
                                  "metric": metric})
        print(filesList)
        d = {"filesList": json.dumps(json.dumps(filesList))}
        src = Template(filein.read())
        result = src.substitute(d)
        with open("index.html", "w") as f:
            f.write(result)
        import SimpleHTTPServer
        import SocketServer

        PORT = 8000

        Handler = SimpleHTTPServer.SimpleHTTPRequestHandler

        httpd = SocketServer.TCPServer(("", PORT), Handler)

        print("serving at port", PORT)
        httpd.serve_forever()
