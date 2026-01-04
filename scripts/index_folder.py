# this file contains a module for using fscrawler from python
import argparse
import json
import subprocess
import utils
import os
from subprocess import Popen, PIPE
import yaml
script_dir = os.path.dirname(os.path.abspath(__file__))
python_path = os.path.join(script_dir, "..", "python", "python.exe")
FSCRAWLER_JOBS = {}
EsClient=utils.get_esclient()
def get_all_jobs():
    """get all jobs of fscrawler"""
    jobs = []
    base_path = utils.get_config("fscrawler")["config_dir"]
    #print(base_path)
    for obj in os.listdir(base_path):
        if os.path.isdir(os.path.join(base_path, obj)) and not obj == "_default":
            jobs.append(obj)
    return jobs


def create_new_job(name: str):
    """create a new project for FSCrawler. this is the first step in running it."""
    print(f"Creating new job: {name}")
    #exe_path = CONFIG["fscrawler"]["exe"]
    config_dir = utils.get_config("docker_env")["FSCRAWLER_CONFIG"]
    current_config_dir = os.path.join(config_dir,name)
    load_defaults_to_job(name)
    print("CREATED DIRECTORY and load default settings AT ", current_config_dir)
    return True
class FscrawlerError (Exception):
    pass

def run_fs_docker_job(name: str,target_dir: str):
    FS_JAVA_OPTS = utils.get_config("docker_env")["FS_JAVA_OPTS"]
    FSCRAWLER_VERSION = utils.get_config("docker_env")["FSCRAWLER_VERSION"]
    FSCRAWLER_PORT = utils.get_config("docker_env")["FSCRAWLER_PORT"]  
    DOCS_FOLDER= os.path.abspath(target_dir)
    FSCRAWLER_CONFIG= os.path.abspath(utils.get_config("docker_env")["FSCRAWLER_CONFIG"])
    print("Running fscrawler docker job", name, "on path", DOCS_FOLDER, "with config", FSCRAWLER_CONFIG)
    subprocess.run(["docker", "rm", "-f", "fs"], stdout=PIPE, stderr=PIPE)
    return subprocess.run(["docker", "run",  "--name", "fs", 
                "--env", f"FS_JAVA_OPTS={FS_JAVA_OPTS}", 
                "-v", f"{DOCS_FOLDER}:{DOCS_FOLDER}:ro", 
                "-v", f"{FSCRAWLER_CONFIG}:/root/.fscrawler",
                "-p", f"{FSCRAWLER_PORT}:8080", 
                
                "--network", "elastic", 
                f"dadoonet/fscrawler:{FSCRAWLER_VERSION}", 
                name, "--restart", "--loop", "1"])

def get_job_settings_path(name: str):
    path = os.path.join(utils.get_config("docker_env")["FSCRAWLER_CONFIG"], name, "_settings.yaml")
    if not os.path.isfile(path):
        raise FscrawlerError("Specified project name doesn't exist, make sure to create it before editing and running: " + path)
    return path 

def load_defaults_to_job_obselete(name: str):
    """Method to load the default settings to a fscrawler job. they are defined in the CONFIG json"""
    # loading everything from the defaults file
    with open(utils.get_config("fscrawler")["defaults"], "r") as f:
        d = yaml.safe_load(f)
    # changing elasticsearch adress to the one in CONFIG
    d["elasticsearch"]["urls"] = utils.get_config("elasticsearch_url")
    # setting name to proper name
    d["name"] = name
    # dumping settings to project dir
    jobDir=os.path.join(utils.get_config("fscrawler")["config_dir"], name)
    settingDir=os.path.join(utils.get_config("fscrawler")["config_dir"], name,"_settings.yaml")
    #os.mkdir(jobDir)
    with open(settingDir, "w") as f:
        yaml.dump(d, f)
    print("Loaded default settings to", settingDir)    
def load_defaults_to_job(name: str):
    """Method to load the default settings to a fscrawler job. they are defined in the CONFIG json"""
    # loading everything from the defaults file
    with open(os.path.join(utils.get_config("docker_env")["FSCRAWLER_CONFIG"], "_defaults.yaml"), "r") as f:
        d = yaml.safe_load(f)
    # changing elasticsearch adress to the one in CONFIG
    elastic_url=f"http://{utils.get_config("docker_env")["CLUSTER_NAME"]}:{utils.get_config("docker_env")["ES_PORT"]}"
    d["elasticsearch"]["urls"] = [elastic_url]
    # setting name to proper name
    d["name"] = name
    # dumping settings to project dir
    jobDir=os.path.join(utils.get_config("docker_env")["FSCRAWLER_CONFIG"], name)
    settingDir=os.path.join(utils.get_config("docker_env")["FSCRAWLER_CONFIG"], name,"_settings.yaml")
    if not os.path.isdir(jobDir):
        os.mkdir(jobDir)
    with open(settingDir, "w") as f:
        yaml.dump(d, f)
    create_job_templates(EsClient,name)
    print("Loaded default settings to", settingDir ," and created templates in elasticsearch") 
def create_job_templates(esclient, newname:str):
    """Method to create fscrawler templates in elasticsearch with a new name"""
    json_path = "fscrawler_templates.json"
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    templates = data.get("component_templates", [])
    comps=[]
    for tpl in templates:
        name = tpl["name"].replace("try1", newname)
        comps.append(name)
        body = tpl["component_template"]

        print(f"Uploading component template: {name}")
        response=EsClient.cluster.put_component_template(        name=name, body=body)
    index_template=data.get("index_templates", [])[0]
    iname = index_template["name"].replace("try1", newname)
    body= index_template["index_template"]  
    body["composed_of"] = comps 
    body["index_patterns"] = [newname] 
    response=esclient.indices.put_index_template(name=iname, body=body)

    print(f" → Elasticsearch response: {response}")    
def get_job_setting(name: str, key: str):
    with open(get_job_settings_path(name), "r") as f:
        ajr = yaml.safe_load(f)
    # change setting
    key_vec = key.split(".")
    if len(key_vec) == 1:
        return ajr[key]
    # if the key is nested than iterate to change
    else:
        curr_d = ajr
        for k in key_vec[:-1]:
            curr_d = curr_d[k]
        return curr_d[key_vec[-1]]

def edit_job_setting(name: str, key: str, value):
    """edit a given setting of the job. can change nested keys by separating dot, for example key=fs.attribute_support"""
    # load file
    with open(get_job_settings_path(name), "r") as f:
        ajr = yaml.safe_load(f)
    # change setting
    key_vec = key.split(".")
    if len(key_vec) == 1:
        ajr[key] = value
    # if the key is nested than iterate to change
    else:
        curr_d = ajr
        for k in key_vec[:-1]:
            curr_d = curr_d[k]
        curr_d[key_vec[-1]] = value
    # write change settings to file
    with open(get_job_settings_path(name), "w") as f:
        yaml.dump(ajr, f)
        print("Saved job settings to", get_job_settings_path(name), "changed", key, "to", value)

def run_job_obselete(name: str):
    """Method to run a fscrawler job. note that it must be pre-configured to run. returns the process object"""
    # before anything we make sure that the job was created
    get_job_settings_path(name)
    # now run the job
    exe_path = utils.get_config("fscrawler")["exe"]
    config_dir = utils.get_config("fscrawler")["config_dir"]
    # we create a job in the specified directory. also, we make sure the crawler will run only once on all files
    cmd = " ".join([exe_path, name, "--config_dir", config_dir, "--loop", "1"])
    print(cmd)
    # run the process, we have to approve the creation by sending "yes"
    p = Popen(cmd, text=True)
    return p


if __name__ == "__main__":
    description="Index all files in a directory to an index using FSCrawler"
    parser = argparse.ArgumentParser(description=description)
   
    parser.add_argument("index_name", help="Name of the Elastic index to create")
    parser.add_argument("path", help="path to the indexed directory")
    parser.add_argument("--new", help="Delete index name and create new one", action="store_true")
    args = parser.parse_args()
    if args.new:
        print(f"Deleting index {args.index_name} if exists")
        EsClient.indices.delete(index=args.index_name, ignore=[400, 404])
    # create a job folder and load defaults
    if create_new_job(args.index_name):            
        edit_job_setting(args.index_name, "fs.url", args.path) 
        edit_job_setting(args.index_name, "fs.ocr.enabled", False)
        run_fs_docker_job(args.index_name, args.path)
    


