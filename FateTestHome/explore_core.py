import json
from jsonpath_ng import parse


def join_paths(regx_path, new_value, dict_replace):
    """
    eg: join_paths(regx_path='$..host..namespace', new_value="9999999999", dict_replace=pydict)
    :param regx_path: the path of replaced key
    :param new_value: the new value of key to be replaced
    :param dict_replace:  the initial_dict that to be replaced
    :return: dict
    """
    data = dict_replace
    jsonpath_expr = parse(regx_path)
    str_path_list = [str(match.full_path) for match in jsonpath_expr.find(dict_replace)]

    def cast_dict_path(path_list):
        cast_list = []
        for str_path in path_list:
            path_split_list = str_path.split('.')
            path = ''
            for i in path_split_list:
                if i.count('[') == 1 and i.count(']') == 1:
                    path = path + '[%s]' % eval(i)[0]
                else:
                    path = path + "['%s']" % i
            cast_list.append(path)
        # [ "['role_parameters']['guest']['args']['data']['train_data'][0]['namespace']" ]
        return cast_list

    cast_paths = cast_dict_path(str_path_list)
    for i in cast_paths:
        if isinstance(new_value, str):
            fullpath = "data" + i + "='%s'" % new_value
            abs_path = fullpath
            exec(abs_path)
        if isinstance(new_value, (int, list, float)):
            fullpath = "data" + i + "={}".format(new_value)
            abs_path = fullpath
            exec(abs_path)
    return data


def muti_replace(params_dict, source_dict: dict):
    """
    format of rep_list:{'$..hetero_lr_0..eps': 0.7777, '$..host..namespace': 8888888}
    for example:
    >> final_dict=muti_replace({'$..hetero_lr_0..eps': 0.7777, '$..host..namespace': 8888888}
    ,source_dict)
     source_dict :the key  need to replaced dict ,type dict
    """
    dict_list = []
    for regx_path,new_value in params_dict.items():
        dict_next = join_paths(regx_path, new_value, dict_replace=source_dict)
        dict_list.append(dict_next)
    for k in dict_list:
        source_dict.update(k)
    # print(json.dumps(source_dict, indent=5))
    return source_dict




