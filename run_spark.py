# -*- coding: utf-8 -*-
import os
import datetime
import subprocess

import exe_dag

SPARK_CMD_INFO = 'sudo -iu hadoop /usr/local/spark/bin/spark-sql ' + \
    '-S --deploy-mode client --name xxxxxxxxxxx --queue xxxxxxxxxx'

print(SPARK_CMD_INFO)
def gen_date_n_before(end_dt, days):
    datelist = []
    for i in range(days):
        datelist.append(str(datetime.datetime.strptime(end_dt, "%Y-%m-%d") - datetime.timedelta(days=i)).split()[0])
    return datelist


def gen_date_from_to(start_dt, end_dt):
    datelist = []
    last = datetime.datetime.strptime(end_dt, "%Y-%m-%d")
    first = datetime.datetime.strptime(start_dt, "%Y-%m-%d")
    if last < first:
        first, last = last, first
    while last >= first:
        datelist.append(str(last).split()[0])
        last = last - datetime.timedelta(days=1)
    return datelist


def execute_task(conf_instance, cmd_instance, task_num=3):
    d = exe_dag.ExeDag(conf_instance, cmd_instance, task_num)
    d.set_retry_span(5)
    d.show_all_deps()
    retcode = d.execute()
    return retcode


def gen_from_model(model_file, start_date, end_date):
    cur = os.getcwd()
    ins = subprocess.Popen('mkdir -p instances', shell=True)
    ins.wait()
    os.chdir(cur+'/instances')
    spark_cmd = '${target}_${date}#' + SPARK_CMD_INFO + \
                ' --hivevar date=${date} -f ${cur}/${target}.sql > ${cur}/instances/${target}_${date}.log 2>&1\n'
    cmd_set = set()
    conf_name = model_file + '__' + start_date + '__' + end_date + '.conf'
    conf_fp = open(conf_name, 'w')
    with open(cur+'/'+model_file) as fp:
        for line in fp:
            segs = line[:-1].split('<<')
            if len(segs) == 2:
                front, back = segs[0].strip(), segs[1].strip()
                for dt in gen_date_from_to(start_date, end_date):
                    dep_head = front + '_' + dt
                    if back.lower() in ('null', 'head'):
                        dep_tail = back
                    else:
                        dep_tail = back + '_' + dt
                    dep = dep_head + '<<' + dep_tail
                conf_fp.write(dep)
                cmd_set.add(front)
                cmd_set.add(back)
    conf_fp.close()
    cmd_name = model_file + '__' + start_date + '__' + end_date + '.txt'
    cmd_fp = open(cmd_name, 'w')
    for cmd in cmd_set:
        if cmd.lower() in ('null', 'head'):
            continue
        for dt in gen_date_from_to(start_date, end_date):
            sql_ins = spark_cmd.replace('${date}', dt).replace('${cur}', cur).replace('${target}', cmd)
            cmd_fp.write(sql_ins)
    cmd_fp.close()
    return conf_name, cmd_name


def run_daily(model_file):
    today = datetime.date.today()
    yesterday = (today + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    conf_file, cmd_file = gen_from_model(model_file, yesterday, yesterday)
    retcode = execute_task(conf_file, cmd_file)
    print(retcode)


def run_history(model_file, start_date, end_date, task_num=3):
    conf_file, cmd_file = gen_from_model(model_file, start_date, end_date)
    retcode = execute_task(conf_file, cmd_file, task_num)
    print(retcode)

