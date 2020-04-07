import queue
import threading
import asyncio
from logutil import logger
import requests
import json
import subprocess
import os
import tempfile
from explore_core import muti_replace
from datetime import datetime

logging = logger("run_task", console_print=True, logging_level=['INFO'], console_debug_level="INFO")
q_init = queue.Queue(maxsize=5)
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


def sub_task(dsl_path, config_path, role, param_test, expect_status):
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
    sub = subprocess.Popen(["python",
                            FATE_FLOW_PATH,
                            "-f",
                            task,
                            "-d",
                            dsl_path,
                            "-c",
                            config_path],
                           shell=False,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT)
    if os.path.exists(dsl.name): print(dsl.name)
    if os.path.exists(conf.name): print(conf.name)
    print("*" * 60)

    stdout, stderr = sub.communicate()
    stdout = stdout.decode("utf-8")
    stdout = json.loads(stdout)
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
        job_status = response["data"]["job"]["fStatus"]
        start_time = response["data"]["job"]["fStartTime"]
        update_time = response["data"]["job"]["fUpdateTime"]
        running_time = 0
        if start_time and update_time:
            st = datetime.utcfromtimestamp(start_time / 1000)
            ud = datetime.utcfromtimestamp(update_time / 1000)
            running_time = (ud - st).seconds
        if job_status in ["success", "failed"]:
            if job_status in ["success", "failed", "canceled"]:
                return response
        else:
            # running ,"waiting"
            if running_time:
                if int(running_time) > ttl:  # >3600*8 hour
                    cmd = f" source {env_path} && python {FATE_FLOW_PATH} -f stop_job -j {job_id}"
                    sub = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    stdout, stderr = sub.communicate()
                    if sub.returncode == 0:
                        if json.loads(stdout.decode("utf-8"))["retcode"] == 0:
                            logging.info(f">>>>>kill job {job_id} by stop_job success>>>>")
                    else:
                        logging.error(stderr)
                    logging.warning(color_str(
                        f">>>>>auto killed job {job_id} Caused by running timeout of setting {ttl} seconds >>>>",
                        "gray"))


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


def worker_consumer(q_init):
    while True:
        if q_init.empty(): break
        conf = q_init.get()
        dsl_path, config_path, param_test, expect_status = conf["dsl_path"]\
            , conf["config_path"], conf["param_test"], conf["expect_status"]
        stdout = sub_task(dsl_path, config_path, role="guest", param_test=param_test, expect_status=expect_status)
        if stdout and stdout["retcode"] == 0:
            job_id = stdout["jobId"]
            rep = future_run(job_id)
            f_status = rep["data"]["job"]["fStatus"]
            elapsed_time = rep["data"]["job"]["fElapsed"]
            signal, colors = None, None
            if f_status in ["success", "failed"]:
                if f_status == expect_status: colors, signal = "green", "OK"
                elif f_status != expect_status: colors, signal = "red", "FALSE"
            else: logging.error(color_str(f"unexpected callback status is {f_status}", "red"))
            logging.info(color_str("%s", colors) % f"{job_id} task finished status is {f_status},"
             f"expect_status:{expect_status},elapsedTime: {elapsed_time/1000} seconds......{signal}")
            q_init.task_done()
        else:
            raise ValueError(color_str("%s", "red") %
                             "submit_job return code not 0,stdout: \n{}".format(stdout)
                             )


def producer(q_init):
    for item in data_conf:
        q_init.put(item)
    q_init.join()
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


def upload_task():
    """build on free hand ssh remote guest and host """
    hook_pwd = os.getcwd()
    command_guest = f"source {env_path}&&cd {hook_pwd}&& python upload_hook.py -role guest"
    command_host = f"ssh {host_ip}  source {env_path}&&cd {hook_pwd}&& python upload_hook.py -role host"
    try:
        print("--" * 30 + "\n" + f"start upload guest:****{guest_ip}\n" + "--" * 30 + "\n")
        sp_guest = subprocess.Popen(command_guest, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out_guest, err_guest = sp_guest.communicate()
        if sp_guest.returncode == 0:
            logging.debug(out_guest.decode())
        else:
            logging.error(err_guest)
    except Exception as e:
        logging.error(e)
    finally:
        try:
            print("--" * 30 + "\n" + f"start upload host:****{host_ip}\n" + "--" * 30 + "\n")
            sp_host = subprocess.Popen(command_host, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out_host, err_host = sp_host.communicate()
            if sp_host.returncode == 0:
                logging.debug(out_host.decode())
            else:
                logging.error(err_host)
        except Exception as f:
            print(f)


def check_table():
    early_stop = 0
    check_cwd = os.getcwd()
    exe_cmd_guest = f"source {env_path} && cd {check_cwd} && python upload_check.py -role guest"
    exe_cmd_host = f"ssh {host_ip} source {env_path} && cd {check_cwd} && python upload_check.py -role host"
    try:
        print("\n" + "--" * 30 + f"\nstart check guest table_info:****{guest_ip}".upper() + "\n" + "--" * 30 + "\n")
        pipe = subprocess.Popen(exe_cmd_guest, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out, err = pipe.communicate()
        if pipe.returncode != 0:
            logging.error(err)
        else:
            msg_list = json.loads(out.decode()).get("table_info")
            for it in msg_list:
                data = it.get("data")
                db, tb, count = data.get("namespace"), data.get("table_name"), data.get("count")
                if count == 0:
                    early_stop += 1
                    logging.error(color_str(f"namespace:{db},table_name:{tb},eggroll count is {count}", "red"))
                else:
                    logging.info(color_str(f"namespace:{db},table_name:{tb},eggroll count is {count}", "green"))
    finally:
        print("--" * 30 + "\n" + f"start check host table_info:****{host_ip}".upper() + "\n" + "--" * 30 + "\n")
        pipe_host = subprocess.Popen(exe_cmd_host, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out_host, err_host = pipe_host.communicate()
        if pipe_host.returncode != 0:
            logging.error(err_host)
        else:
            msg_list_host = json.loads(out_host.decode()).get("table_info")
            for it in msg_list_host:
                data_a = it.get("data")
                db_a, tb_a, count_a = data_a.get("namespace"), data_a.get("table_name"), data_a.get("count")
                if count_a == 0:
                    early_stop += 1
                    logging.error(color_str(f"namespace:{db_a},table_name:{tb_a},eggroll count is {count_a}", "red"))
                else:
                    logging.info(color_str(f"namespace:{db_a},table_name:{tb_a},eggroll count is {count_a}", "green"))
    if early_stop: raise NameError("check upload false")


if __name__ == '__main__':
    upload_task()
    print("--" * 30 + "\n" + "->Start check ${namespace} ${table_info} uploaded ...\n" + "--" * 30 + "\n")
    check_table()
    print("--" * 30 + "\n" + "finish all guest and host upload check".upper() + "\n" + "--" * 30 + "\n")
    try:
        producer = [threading.Thread(target=producer, args=(q_init,))]
        consumer = [threading.Thread(target=worker_consumer, args=(q_init,)) for i in range(5)]
        consumer_pool = []
        producer_pool = []
        for p in producer:
            p.start()
            producer_pool.append(p)
        for k in consumer:
            k.start()
            consumer_pool.append(k)
        for m in consumer:
            m.join()
        for p in producer_pool:
            p.join()
        logging.info("=" * 30 + "->exit ......")
    except Exception as e:
        logging.error(e)
