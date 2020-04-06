import json

conf_dict = json.load(open("./demo_conf/basic_data_conf.json", "r+"))
job_conf_list = conf_dict.get("conf")


def params_factory(save_path):
    fp = open(save_path, "w+", encoding="utf-8")
    data = {"conf": []}
    for job_conf in job_conf_list:
        dsl = job_conf["dsl_path"]
        conf = job_conf["config_path"]
        algorithm_type = job_conf["algorithm_type"]
        params_test = job_conf["params_test"]
        for one_replace_dict in params_test:
            one_job_conf = {"dsl_path": dsl, "config_path": conf,
                            "algorithm_type": algorithm_type,
                            "param_test": one_replace_dict.get("params"),
                            "expect_status": one_replace_dict.get("expect_status")
                            }
            data.get("conf").append(one_job_conf)
    json.dump(data, fp, indent=4)
    fp.close()


if __name__ == '__main__':
    save_conf_path = "./demo_conf/data_conf.json"
    params_factory(save_conf_path)
