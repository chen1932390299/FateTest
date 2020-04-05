import argparse
import tempfile
import json
import os
import subprocess
SSH_CONF=json.load(open("./demo_conf/ssh_conf.json","r+"))
FLOW_HOME=SSH_CONF.get("FLOW_HOME")
ENV_PATH=SSH_CONF.get("env_path")
file_conf=json.load(open("./demo_conf/files_conf.json","r+")).get("file_conf")
FATE_FLOW_PATH =SSH_CONF.get("FATE_FLOW_PATH")
def upload_util():
    parser =argparse.ArgumentParser()
    parser.add_argument("-role","--role",required=True,type=str,help="the role of you upload party")
    args=parser.parse_args()
    operate_role=args.role
    for cf in file_conf:
        role_file_conf=cf.get(operate_role)
        with tempfile.NamedTemporaryFile(delete=True, dir="./examples", suffix=".json") as tmpfile:
            file_name = tmpfile.name  # /example/xxx.json
            tmpfile.write(json.dumps(role_file_conf, indent=2).encode())
            tmpfile.seek(0)
            if os.path.exists(file_name):
           
                cmd =f"source {ENV_PATH}&& python {FATE_FLOW_PATH} -f upload -c {file_name}"
            
                sp=subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                stdout,stderr=sp.communicate()
                if sp.returncode !=0:
                    raise ValueError(f" error take {stderr}")
                else:
                    stdout_str =json.loads(stdout)
                    stdout_format=json.dumps(stdout_str,indent=3)
                    print(stdout_format)
            else: raise ValueError(f"not found {file_name}")


if __name__ == '__main__':
    upload_util()

