from __future__ import print_function
import sys
import os
from assets import PythonAsset
from quark_lib.beats import Beat
from StringIO import StringIO
import json
import urllib
import webbrowser
try:
    import pycurl
except:
    print("Need pycurl dependency to use qubole as the deployment platform. Run pip install pycurl in your virtualenv and try this again.")
    sys.exit(1)

class Qubole:
    def __init__(self, config, options):
        self.config = config
        self.options = options
        projectsDir = self.config.get(self.options.env, "projects_dir")
        schemasDir = os.path.join(projectsDir, "schemas")
        self.beats = Beat(file(os.path.join(schemasDir, "beats.schema.json")).read())

    def _q_config(self,item):
        return self.config.get(self.options.env, "qubole-{}".format(item))

    def _do_request(self, method, path, base_url=None,  **data):
        # Uh, only using pycurl because that was the example that was around, will port to requests someday
        # it's supposed to be faster, so oh well
        c = pycurl.Curl()
        auth_token = self._q_config("auth_token")
        if base_url == None:
            base_url = self.config.get(self.options.env, "master")
        url = base_url+ "/" + path
        buffer = StringIO()
        c.setopt(c.WRITEDATA, buffer)
        print("Using", url, file=sys.stderr)
        c.setopt(pycurl.URL, url)
        c.setopt(pycurl.HTTPHEADER, ["X-AUTH-TOKEN: "+ auth_token, "Content-Type:application/json", "Accept: application/json, text/plain"])
        ## Note: Only POST and GET have been tested...
        ## It's not very obvious with pycurl to do this properly with PUT and DELETE
        ## Review this if ever needed to add these methods
        ## http://www.programcreek.com/python/example/2132/pycurl.HTTPHEADER
        if method.lower() == "post":
            c.setopt(pycurl.POST,1)
            post_data = json.dumps(data)
            c.setopt(pycurl.POSTFIELDS, post_data)
        elif method.lower() == "get":
            c.setopt(pycurl.HTTPGET, 1)
        elif method.lower() == "delete":
            c.setopt(pycurl.DELETE, 1)
        elif method.lower() == "put":
            #c.setopt(pycurl.UPLOAD, 1)
            post_data = json.dumps(data)
            print(post_data)
            c.setopt(pycurl.CUSTOMREQUEST, "PUT")
            c.setopt(pycurl.POSTFIELDS, post_data)
        elif method.lower() == "head":
            c.setopt(pycurl.NOBODY,1)
        else:
            print("Unknown method ", method)
            sys.exit(1)
        c.perform()
        c.close()
        body = buffer.getvalue()
        return body

    def _get_cluster_id(self):
        cluster_id = self._q_config("cluster_id")
        assert cluster_id is not None
        return cluster_id


    def invoke_task(self,name, *args):
        if args == (None,):
            getattr(self,name)()
        else:
            getattr(self,name)(*args)

    def deploy(self, asset_path, *args):
        prog = file(asset_path).read()
        resp_body = self._do_request("POST", "commands",
                                     program=prog,language="python",
                                     command_type="SparkCommand",
                                     label=self._q_config("cluster_label"),
                                     arguments="--py-files {}".format(self._q_config("py_files")),
                                     user_program_arguments=' '.join(args))
        #print(resp_body)
        response = json.loads(resp_body)
        print(response)
        job_id = response["id"]
        job_url = "https://api.qubole.com/v2/analyze?command_id={}".format(job_id)
        print
        print
        print("started job with id {}. You can track the job at {}".format(job_id, job_url))

    def logs(self, job_id):
        assert job_id is not  None
        resp_body = self._do_request("GET", "commands/{}/logs".format(job_id))
        print(resp_body)

    def results(self, job_id):
        assert job_id is not None
        resp_body = self._do_request("GET", "commands/{}/results".format(job_id))
        print(resp_body)

    def status(self, job_id):
        assert job_id is not None
        resp_body = self._do_request("GET", "commands/{}".format(job_id))
        print(resp_body)
        #resp = json.loads(resp_body)
        #print("Status:", resp["status"])
        #print("Progress:", resp["progress"])
        #print(resp_body)

    def _master(self, cluster_id):
        assert cluster_id is not None
        resp_body = self._do_request("GET", "clusters/{}/state".format(cluster_id), base_url= "https://api.qubole.com/api/v1.3")
        j = json.loads(resp_body)
        for node in j["nodes"]:
            print(node)
            if node["role"] == u"master":
                return node["hostname"]
        return None

    def master(self):
        cluster_id =self._get_cluster_id()
        assert cluster_id is not None
        print(self._master(cluster_id))

    def notebook(self):
        webbrowser.open("https://api.qubole.com/spark-notebook-{}/".format(self._get_cluster_id()))

    def history(self):
        master = self._master(self._get_cluster_id())
        encoded = urllib.urlencode({"encodedUrl": "http://{}:18080".format(master)})
        webbrowser.open("https://api.qubole.com/cluster-proxy?{}".format(encoded))

    def ganglia(self):
        webbrowser.open("https://api.qubole.com/ganglia-metrics-{}/".format(self._get_cluster_id()))

    def metrics(self, metric):
        cluster_id = self._get_cluster_id()
        resp_body = self._do_request("GET", "clusters/{}/metrics".format(cluster_id), base_url= "https://api.qubole.com/api/v1.3")
        j = json.loads(resp_body)
        print(json.dumps(j, sort_keys=True, indent=4))

    def describecluster(self):
        cluster_id = self._get_cluster_id()
        resp_body = self._do_request("GET", "clusters/{}".format(cluster_id), base_url= "https://api.qubole.com/api/v1.3")
        j = json.loads(resp_body)
        print(json.dumps(j, sort_keys=True, indent=4))


    def lsclusters(self):
        resp_body = self._do_request("GET", "clusters", base_url= "https://api.qubole.com/api/v1.3")
        j = json.loads(resp_body)
        for cluster in j["clusters"]:
            labels = cluster["label"]
            print(cluster["id"], labels)

        #print(resp_body)

    def lsschedules(self):
        resp_body = self._do_request("GET", "scheduler")
        print(resp_body)

    def schedule(self, asset_path, schedule_id, schedule_iso8601):
        prog = file(asset_path).read()
        # TODO: refactor to support other languages
        command_json = {"program": prog,
                                   "language": "python",
                                   "arguments": "--py-files {}".format(self._q_config("py_files")),
                                   "user_program_arguments": "--config {}".format(self._q_config("config_path"))
        }
        resp_body = self._do_request("PUT", "scheduler/{}".format(schedule_id),
                                     command=command_json,language="python",
                                     command_type="SparkCommand",
                                     template="genetic",
                                     time_out=1440,
                                     frequency=60,
                                     time_unit="minutes",
                                     label=self._q_config("cluster_label"),

                                     )
        print(resp_body)
