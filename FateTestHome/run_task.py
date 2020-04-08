import queue
import threading
import asyncio
from logutil import logger
import requests
import json
import subprocess
import os,time
import tempfile
from explore_core import muti_replace
from datetime import datetime

logging = logger("run_task", console_print=True, logging_level=['INFO'], console_debug_level="INFO")
job_queue = queue.Queue(maxsize=5)
with open("./demo_conf/ssh_conf.json", "r+")as f:
    ssh_conf = json.load(f)
guest_ip = ssh_conf.get("guest_ip")
host_ip = ssh_conf.get("host_ip")
env_path = ssh_conf.get("env_path")
FATE_FLOW_PATH = ssh_conf.get("FATE_FLOW_PATH")
part_id_guest = ssh_conf.get("guest_part_id")
data_conf = json.load(open("./demo_conf/data_conf.json", "r+")).get("conf")
ttl = ssh_conf.get("JOB_TIMEOUT_SECONDS")
files_conf = json.load(open("./demo_conf/files_conf.json", "r+")).get("file_conf")


def exc_cmd(args,shell_switch,decoder=True):
    sub = subprocess.Popen(args,
                           shell=shell_switch,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT)
    stdout,stderr=sub.communicate()
    try:
        if decoder:
            stdout=json.loads(stdout.decode("utf-8"))
            return stdout
        else: return stdout
    except json.decoder.JSONDecodeError:
        raise ValueError(stdout)


def sub_task(dsl_path, config_path, role, param_test):
    task = "submit_job"
    with open(dsl_path, "r+") as f:
        dsl_dict = json.load(f)
    with open(config_path, "r+") as e:
        conf_ctx = json.load(e)
        conf_dict = muti_replace(param_test, conf_ctx)
    dsl = tempfile.NamedTemporaryFile("w+b", suffix=".json", dir="./examples", delete=True)
    conf = tempfile.NamedTemporaryFile("w+b", suffix=".json", dir="./examples", delete=True)
    dsl.write(json.dumps(dsl_dict, indent=2).encode())
    conf.write(json.dumps(conf_dict, indent=2).encode())
    dsl_path, config_path = dsl.name, conf.name
    dsl.seek(0)
    conf.seek(0)
    args=["python",FATE_FLOW_PATH,"-f",task,"-d",dsl_path,"-c",config_path]
    stdout = exc_cmd(args,shell_switch=False)
    status = stdout["retcode"]
    dsl.close()
    conf.close()
    if status != 0:
        tip = f"[exec_task] task_type:{task}, role:{role} exec fail, status:{status}, stdout:{stdout}"
        raise ValueError(
            color_str(tip, "red")
        )
    message = color_str("%s", "green") % f"[exec_task] task_type:{task}" \
                                         f", role:{role} exec success, stdout:\n{json.dumps(stdout, indent=3)}"
    logging.info(message)

    return stdout


async def jobs(job_id):
    while True:
        res = requests.get(url=f"http://{guest_ip}:8080/job/query/{job_id}/guest/{part_id_guest}")
        response = res.json()
        job_data=response["data"]["job"]
        job_status,start_time,update_time = job_data["fStatus"],job_data["fStartTime"],int(time.time()*1000)
        running_time = 0
        if start_time and update_time:
            st = datetime.utcfromtimestamp(start_time / 1000)
            ud = datetime.utcfromtimestamp(update_time / 1000)
            running_time = (ud - st).seconds
        if job_status in ["success", "failed","canceled"]:return response
        else: # running ,"waiting"
            if running_time and int(running_time) > ttl:
                cmd = f" source {env_path} && python {FATE_FLOW_PATH} -f stop_job -j {job_id}"
                stdout=exc_cmd(cmd,shell_switch=True)
                if stdout["retcode"] == 0:logging.warning(color_str(
                    f"auto killed job {job_id},has run {running_time} seconds ,timeout of setting {ttl} seconds >>>>","gray"))


async def do_work(job_id):
    msg = await jobs(job_id)
    return msg


def future_run(job_id):
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(do_work(job_id))
    loop.run_until_complete(asyncio.wait([task]))
    loop.close()
    result = task.result()
    return result


class ConsumerJobConf(threading.Thread):
    def __init__(self,queue):
        super().__init__()
        self.queue = queue

    def run(self):
        while True:
            if self.queue.empty(): break
            conf = self.queue.get()
            dsl_path, config_path, param_test, expect_status = conf["dsl_path"] \
                , conf["config_path"], conf["param_test"], conf["expect_status"]
            stdout = sub_task(dsl_path, config_path, role="guest", param_test=param_test)
            if stdout and stdout["retcode"] == 0:
                job_id = stdout["jobId"]
                rep = future_run(job_id)
                f_status = rep["data"]["job"]["fStatus"]
                elapsed_time = rep["data"]["job"]["fElapsed"]
                signal, colors = None, None
                if f_status in ["success", "failed"]:
                    if f_status == expect_status:
                        colors, signal = "green", "OK"
                    elif f_status != expect_status:
                        colors, signal = "red", "FALSE"
                else:
                    logging.error(color_str(f"unexpected callback status is {f_status}", "red"))
                logging.info(color_str("%s", colors) % f"{job_id} task finished status is {f_status},"
                                                       f"expect_status:{expect_status},elapsedTime: {elapsed_time / 1000} seconds......{signal}")
                self.queue.task_done()
            else:
                raise ValueError(color_str("%s", "red") %
                                 "submit_job return code not 0,stdout: \n{}".format(stdout)
                                 )


class ProducerJobConf(threading.Thread):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def run(self):
        for item in data_conf:
            self.queue.put(item)
        self.queue.join()
        print("=" * 50 + "\n")
        logging.info("=" * 30 + "->finish all tasks.....+\n")


def color_str(tip, color):
    # todo define three color: red,green,blue,gray.
    if color == "red":
        return "\033[31m%s\033[0m" % tip
    elif color == "green":
        return "\033[32m%s\033[0m" % tip

    elif color == "blue":
        return "\033[34m%s\033[0m" % tip

    elif color == "gray":
        return "\033[35m%s\033[0m" % tip


def upload_func(operate_role):
    if operate_role=="guest":
        print("--" * 30 + "\n" + f"start upload guest:****{guest_ip}\n" + "--" * 30 + "\n")
        hook_pwd = os.getcwd()
        command_guest = f"source {env_path}&&cd {hook_pwd}&& python upload_hook.py -role guest"
        stdout=exc_cmd(command_guest,True,False)
        logging.debug(stdout.decode())
    if operate_role=="host":
        print("--" * 30 + "\n" + f"start upload host:****{host_ip}\n" + "--" * 30 + "\n")
        for cf in files_conf:
            role_file_conf = cf.get(operate_role)
            if role_file_conf:
                with tempfile.NamedTemporaryFile("w",delete=True,suffix=".json") as f:
                    json.dump(role_file_conf, f)
                    f.flush()
                    st=subprocess.Popen(["scp", "-q",f.name, f"{host_ip}:{f.name}"],shell=False)
                    upload_cmd = " && ".join([f"source {env_path}",
                                              f"python {FATE_FLOW_PATH} -f upload -c {f.name}",
                                              f"rm {f.name}"])
                    stdout=exc_cmd(["ssh", host_ip, upload_cmd],False)
                    logging.debug(stdout)

            else:pass


def upload_task():
    try:
        upload_func("guest")
    finally:upload_func("host")


def check_func(role):
    if role == "guest":
        check_cwd = os.getcwd()
        cmd = f"source {env_path} && cd {check_cwd} && python upload_check.py -role guest"
        print("\n" + "--" * 30 + f"\nstart check guest table_info:****{guest_ip}".upper() + "\n" + "--" * 30 + "\n")
        stdout = exc_cmd(cmd,True)
        msg_list = stdout.get("table_info")
        for it in msg_list:
            data = it.get("data")
            db, tb, count = data.get("namespace"), data.get("table_name"), data.get("count")
            if count == 0:
                raise ValueError(color_str(f"namespace:{db},table_name:{tb},eggroll count is {count}", "red"))
            else:
                logging.info(color_str(f"namespace:{db},table_name:{tb},eggroll count is {count}", "green"))

    if role == "host":
        print("--" * 30 + "\n" + f"start check host table_info:****{host_ip}".upper() + "\n" + "--" * 30 + "\n")
        for cf in files_conf:
            role_file_conf = cf.get(role)
            if role_file_conf:
                namespace = role_file_conf.get("namespace")
                table_name = role_file_conf.get("table_name")
                query_table_cmd = " && ".join([f"source {env_path}",
                                          f"python {FATE_FLOW_PATH} -f table_info -n {namespace} -t {table_name}"
                                          ])
                stdout=exc_cmd(["ssh", host_ip, query_table_cmd],False)
                data=stdout.get("data")
                count,tag=data.get("count"),None
                if count ==0: raise ValueError(color_str(f"namespace: {namespace}, table_name: {table_name} count:{count}","red"))
                else:logging.info(color_str(f"namespace: {namespace}, table_name: {table_name} count:{count}","green"))
            else:pass


def check_table():
    try:
        check_func("guest")
    finally:
        check_func("host")


def test_suite():
    upload_task()
    print("--" * 30 + "\n" + "->Start check ${namespace} ${table_info} uploaded ...\n" + "--" * 30 + "\n")
    check_table()
    print("--" * 30 + "\n" + "finish all guest and host upload check".upper() + "\n" + "--" * 30 + "\n")
    producer,consumer=[],[]
    for p in range(1):
        producer_thread=ProducerJobConf(job_queue)
        producer_thread.start()
        producer.append(producer_thread)
    for c in range(job_queue.qsize()):
        consumer_thread=ConsumerJobConf(job_queue)
        consumer_thread.start()
        consumer.append(consumer_thread)
    for c in consumer:
        c.join()
    for p in producer:
        p.join()
    logging.info("=" * 30 + "->exit ......")


if __name__ == '__main__':
    test_suite()

