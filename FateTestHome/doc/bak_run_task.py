import queue
import threading
import asyncio
from logutil import logger
import requests
import json
import subprocess
import tempfile
import os
from datetime import datetime
logging = logger("aa", console_print=True, logging_level=['INFO'],console_debug_level="INFO")
q_init = queue.Queue(maxsize=5)
with open("./demo_conf/ssh_conf.json","r+")as f:
    ssh_conf=json.load(f)
guest_ip=ssh_conf.get("guest_ip")
host_ip=ssh_conf.get("host_ip")
env_path=ssh_conf.get("env_path")
# FLOW_HOME=ssh_conf.get("FLOW_HOME")
FATE_FLOW_PATH = ssh_conf.get("FATE_FLOW_PATH")
partid_guest=ssh_conf.get("guest_part_id")
data_conf=json.load(open("./demo_conf/data_conf.json","r+")).get("conf")
ttl=ssh_conf.get("JOB_TIMEOUT_SECONDS")
files_conf=json.load(open("./demo_conf/files_conf.json","r+")).get("file_conf")
def sub_task(dsl_path,config_path,role):

        task = "submit_job"
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

        stdout, stderr = sub.communicate()
        stdout = stdout.decode("utf-8")
        stdout = json.loads(stdout)
        status = stdout["retcode"]
        if status != 0:
            tip=f"[exec_task] task_type:{task}, role:{role} exec fail, status:{status}, stdout:{stdout}"
            raise ValueError(
                color_str(tip,"red")
                )
        message=color_str("%s","green")%f"[exec_task] task_type:{task}, role:{role} exec success, stdout:\n{json.dumps(stdout,indent=3)}"
        logging.info(message)
        return stdout


async def jobs(job_id):
    guestIp=guest_ip # todo set guest_board ip
    jobId=job_id
    part_id_guest=partid_guest
    while True:
        res = requests.get(url=f"http://{guestIp}:8080/job/query/{jobId}/guest/{part_id_guest}")
        response = res.json()

        job_status = response["data"]["job"]["fStatus"]
        # duration=response["data"]["job"]["fElapsed"]  #int
        start_time=response["data"]["job"]["fStartTime"]
        update_time=response["data"]["job"]["fUpdateTime"]
        running_time=0
        if start_time and update_time:
          st=datetime.utcfromtimestamp(start_time/1000)
          ud=datetime.utcfromtimestamp(update_time/1000)
          running_time=(ud-st).seconds
        if job_status in ["success","failed","waiting"]:
            if job_status in ["success","failed"]:
                return response
            else:
                continue
        else:
            if running_time:
                if int(running_time)>ttl: # >3600*8 hour
                    cmd=f" source {env_path} && python {FATE_FLOW_PATH} -f stop_job -j {job_id}"
                    sub =subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                    stdout, stderr = sub.communicate()
                    if sub.returncode ==0:
                        if json.loads(stdout.decode("utf-8"))["retcode"]==0:
                           logging.info(f">>>>>kill job {job_id} by stop_job success>>>>")
                    else: logging.error(stderr)
                    logging.warning(color_str(f">>>>>auto killed job {job_id} Caused by running timeout of setting {ttl} seconds >>>>","gray"))


async def do_work(job_id):
    msg=await jobs(job_id)
    return msg


def furture_run(job_id):
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
        if q_init.empty():
            break
        conf = q_init.get()
        dsl_path,config_path,role=conf["dsl_path"],conf["config_path"],conf["role_guest"]
        stdout=sub_task(dsl_path,config_path,role)
        if stdout and stdout["retcode"]==0:
            job_id=stdout["jobId"]
            rep= furture_run(job_id)
            f_status=rep["data"]["job"]["fStatus"]
            elpasedTime=rep["data"]["job"]["fElapsed"]
            if f_status in ["success", "failed","running"]:
                signal=None
                if f_status =="success":
                    colors="green"
                    signal="OK"
                elif f_status =="failed":
                    colors="red"
                    signal="FALSE"
                else:
                    colors="red"
                    logging.error(f"unexpected callback status is {f_status}")
                logging.info(
                    color_str("%s",colors)%
                    "%s task finished status is %s,elapsedTime %s seconds.....%s" % (job_id,f_status,elpasedTime/1000,signal)
                )
                q_init.task_done()
        else:
            raise ValueError(color_str("%s","red")%
                "submit_job return_code not 0,stdout:\n{} ".format(stdout)
            )


def producer(q_init):
    for item in data_conf:
        q_init.put(item)

    q_init.join()
    print("="*50+"\n")
    logging.info("="*30+"->finish all tasks.....+\n")

def color_str(tip, color):
    # todo define three color: red,green,blue,gray.
    if color == "red":
        return "\033[31m%s\033[0m" % tip
    elif color == "green":
        return "\033[32m%s\033[0m" % tip

    elif color == "blue":
        return "\033[34m%s\033[0m" % tip

    elif color == "gray":
        return "\033[35m%s\033[0m"%tip

def upload_task():
    """build on free hand ssh remote guest and host """
    hook_pwd=os.getcwd()
    COMMAND_GUEST=f"source {env_path}&&cd {hook_pwd}&& python upload_hook.py -role guest"
    COMMAND_HOST=f"ssh {host_ip}  source {env_path}&&cd {hook_pwd}&& python upload_hook.py -role host"

    try:
        sp_guest = subprocess.Popen(COMMAND_GUEST, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out_guest, err_guest = sp_guest.communicate()
        if sp_guest.returncode ==0:
            out_guest_format=json.loads(out_guest.decode())
            msg=f"****{guest_ip},task_type:upload_guest:\n{json.dumps(out_guest_format,indent=3)}"
            logging.info(color_str(msg,"green"))
        else:
            err_guest_format=json.loads(err_guest.decode())
            err_msg=f"****{guest_ip},task_type:uplaod_guest:\n{json.dumps(err_guest_format,indent=3)}"
            logging.error(color_str(err_msg,"red"))
    except Exception as e:
        print(e)
    finally:
        try:
            sp_host = subprocess.Popen(COMMAND_HOST, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out_host,err_host=sp_host.communicate()
            if sp_host.returncode ==0:
                out_host_format=json.loads(out_host.decode())
                out_host_msg=f"#####{host_ip},task_type:upload_host:\n{json.dumps(out_host_format,indent=3)}"
                logging.info(color_str(out_host_msg,"green"))
            else:
                err_host_format=err_host.decode()
                err_host_msg=f"#####{host_ip},task_type:upload_host:\n{err_host_format}"
                logging.error(color_str(err_host_msg,"red"))
        except Exception as f:
            print(f)


if __name__ == '__main__':
    try:
        upload_task()
    except Exception as e:
        pass
    else:
        print("--"*30+"\n")
        print(f"="*30+"->finish upload guest and host data_set...\n")
        print("--" * 30 + "\n")

    # finally:
    #     try:
    #         producer = [threading.Thread(target=producer,args=(q_init,))]
    #         consumer = [threading.Thread(target=worker_consumer, args=(q_init,)) for i in range(5)]
    #         consumer_pool = []
    #         producer_pool=[]
    #         for p in producer:
    #             p.start()
    #             producer_pool.append(p)
    #         for k in consumer:
    #             k.start()
    #             consumer_pool.append(k)
    #         for m in consumer:
    #             m.join()
    #         for p in producer_pool:
    #             p.join()
    #         logging.info("="*30+"->exit ......")
    #     except Exception as e:
    #         logging.error(e)

