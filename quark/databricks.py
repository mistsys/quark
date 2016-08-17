from __future__ import print_function, absolute_import
from .beats import Beat
from StringIO import StringIO
import sys
import os
import json
import urllib
import webbrowser
try:
    import pycurl
except:
    print("Need pycurl dependency to use qubole as the deployment platform. Run pip install pycurl in your virtualenv and try this again.")
    sys.exit(1)

class Databricks:
    def __init__(self, config, options):
        self.config = config
        self.options = options
        projectsDir = self.config.get(self.options.env, "projects_dir")
        schemasDir = os.path.join(projectsDir, "schemas")
        schemasFile = os.path.join(schemasDir, "beats.schema.json")
        if os.path.exists(schemasFile):
            self.beats = Beat(file(schemasFile).read())

    def _q_config(self,item):
        return self.config.get(self.options.env, "databricks-{}".format(item))

    def _do_request(self, method, path, base_url=None,  **data):
        # Uh, only using pycurl because that was the example that was around, will port to requests someday
        # it's supposed to be faster, so oh well
        c = pycurl.Curl()

        #auth_token = self._q_config("auth_token")
        username = self._q_config("username")
        password = self._q_config("password")
        if base_url == None:
            base_url = self.config.get(self.options.env, "master")
        url = base_url+ "/" + path
        buffer = StringIO()
        c.setopt(c.WRITEDATA, buffer)
        print("Using", url, file=sys.stderr)
        c.setopt(pycurl.URL, url)
        c.setopt(pycurl.HTTPHEADER, ['Accept:application/json'])
        #c.setopt(pycurl.HTTPHEADER, ["X-AUTH-TOKEN: "+ auth_token, "Content-Type:application/json", "Accept: application/json, text/plain"])
        ## Note: Only POST and GET have been tested...
        ## It's not very obvious with pycurl to do this properly with PUT and DELETE
        ## Review this if ever needed to add these methods
        ## http://www.programcreek.com/python/example/2132/pycurl.HTTPHEADER
        if method.lower() == "post":
            c.setopt(pycurl.POST,1)
            post_data = urllib.urlencode(data)
            print(post_data)
            c.setopt(pycurl.POSTFIELDS, post_data)
        elif method.lower() == "get":
            c.setopt(pycurl.HTTPGET, 1)
        elif method.lower() == "delete":
            c.setopt(pycurl.DELETE, 1)
        elif method.lower() == "put":
            #c.setopt(pycurl.UPLOAD, 1)
            post_data =  urllib.urlencode(data)
            c.setopt(pycurl.CUSTOMREQUEST, "PUT")
            c.setopt(pycurl.POSTFIELDS, post_data)
        elif method.lower() == "head":
            c.setopt(pycurl.NOBODY,1)
        else:
            print("Unknown method ", method)
            sys.exit(1)
        if username != None and password != None:
            c.setopt(pycurl.USERPWD, '%s:%s' % (username, password))
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
        # Use multipart upload to libraries/upload
        print("TBD")

    def logs(self, job_id):
        print("TBD")


    def status(self, job_id):
        print("TBD")


    def notebook(self):
        print("TBD")


    def _get_clusters(self):
        resp_body = self._do_request("GET", "clusters/list")
        j = json.loads(resp_body)

        return j

    def describecluster(self, name):
        clusters = self._get_clusters()
        for cluster in clusters:
            if cluster['name'] == name:
                print(cluster)

    def lsclusters(self):
        clusters = self._get_clusters()
        if len(clusters) == 0:
            print("No clusters created")
        for cluster in clusters:
            print(cluster)

    def mkcluster(self, name, memory_gb=6, use_spot=True):
        resp_body = self._do_request("POST", "clusters/create", name=name,
                                     memoryGB=memory_gb,
                                     useSpot=use_spot
        )
        print(resp_body)

    def lslibraries(self):
        resp_body = self._do_request("GET", "libraries/list")
        j = json.loads(resp_body)

        print(j)

    def describelibraries(self):
        resp_body = self._do_request("GET", "libraries/status")
        j = json.loads(resp_body)
        print(j)

    def rmlibrary(self, library_id):
        resp_body = self._do_request("DELETE", "clusters/create", libraryId=library_id)
        print(resp_body)

    def attachlibrary(self, library_id, cluster_id):
        print("TBD")


    def schedule(self, asset_path, schedule_id, schedule_iso8601):
        print("TBD")
