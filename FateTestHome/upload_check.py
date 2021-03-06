import argparse
import json
import subprocess

SSH_CONF = json.load(open("./demo_conf/ssh_conf.json", "r+"))
ENV_PATH = SSH_CONF.get("env_path")
file_conf = json.load(open("./demo_conf/files_conf.json", "r+")).get("file_conf")
FATE_FLOW_PATH = SSH_CONF.get("FATE_FLOW_PATH")


def check_table_info():
    schema_info = {"table_info": []}
    parser = argparse.ArgumentParser()
    parser.add_argument("-role", "--role", required=True, type=str, help="the role of you check table_info party")
    args = parser.parse_args()
    operate_role = args.role
    for cx in file_conf:
        role_file_conf = cx.get(operate_role)
        if role_file_conf:
            namespace = role_file_conf.get("namespace")
            table_name = role_file_conf.get("table_name")
            exe_cmd = f"source {ENV_PATH}&& python {FATE_FLOW_PATH} -f table_info -n {namespace} -t {table_name}"
            pipe = subprocess.Popen(exe_cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)
            out, err = pipe.communicate()
            if pipe.returncode != 0:
                raise ValueError(err)
            else:
                out_dict = json.loads(out)
                schema_info.get('table_info').append(out_dict)
        else:pass
    print(json.dumps(schema_info, indent=3))


if __name__ == '__main__':
    check_table_info()
