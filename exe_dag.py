# -*- coding: utf-8 -*-
import time
import logging
from queue import Queue

from dag_node import TaskNode
from worker import ShellWorker

FREE = 0
BUSY = 1


class ExeDag(object):
    def __init__(self, dag_file, cmd_file, worker_num):
        self.nodedict = {}
        self.parse_dag_file(dag_file)
        self.parse_cmd_file(cmd_file)
        self.wait_list = []
        self.ready_queue = Queue(maxsize=0)
        self.worker_list = []
        for i in range(worker_num):
            worker = ShellWorker(i)
            self.worker_list.append(worker)
        self.fail_queue = Queue(maxsize=0)
        self.success_queue = Queue(maxsize=0)
        # self.retry_queue = Queue(maxsize=0)
        self.retry_times = 3
        self.retry_span = 60

    def set_retry_times(self, times):
        self.retry_times = times

    def set_retry_span(self, span):
        self.retry_span = span

    def add_node_by_name(self, name, node):
        self.nodedict[name] = node

    def get_node_by_name(self, name):
        if name in self.nodedict:
            return self.nodedict[name]
        return

    def show_all_deps(self):
        logging.info('-' * 16)
        for n in self.nodedict:
            logging.info(n + self.nodedict[n].get_cmd() + '::')
            deps = self.nodedict[n].get_deps()
            logging.info(deps)
        logging.info('-' * 16)

    def parse_dag_file(self, dag_file):
        with open(dag_file) as fp:
            for line in fp:
                segs = line.split('<<')
                if len(segs) == 2:
                    back = segs[0].strip()
                    front = segs[1].strip()
                    if not back:
                        continue
                    if not front:
                        continue
                    if not self.get_node_by_name(back):
                        self.add_node_by_name(back, TaskNode(back))
                    if not self.get_node_by_name(front):
                        self.add_node_by_name(front, TaskNode(front))
                    self.get_node_by_name(back).add_dep(front)

    def parse_cmd_file(self, cmd_file):
        with open(cmd_file) as fp:
            for line in fp:
                segs = line.split('#')
                if len(segs) == 2:
                    name = segs[0].strip()
                    cmd = segs[1].strip()
                    if self.get_node_by_name(name):
                        self.get_node_by_name(name).set_cmd(cmd)

    def remove_dep(self, node):
        name = node.get_name()
        for n in self.wait_list:
            n.remove_dep(name)

    def all_node_to_wait(self):
        self.wait_list = self.nodedict.values()
        for node in self.wait_list:
            node.set_retry_times(self.retry_times)

    def wait_to_ready(self):
        tmp_list = []
        for node in self.wait_list:
            if not node.get_deps():
                self.ready_queue.put(node)
            else:
                tmp_list.append(node)
        self.wait_list = tmp_list

    def worker_period(self):
        for worker in self.worker_list:
            worker.check_status()
            if worker.get_status() == FREE:
                # 执行后分发到下游队列
                if worker.get_is_success():
                    if worker.get_cur_node():
                        success_node = worker.get_cur_node()
                        self.remove_dep(success_node)
                        self.success_queue.put(success_node)
                        worker.clean()
                        self.wait_to_ready()
                else:
                    fail_node = worker.get_cur_node()
                    chance = fail_node.get_retry_times()
                    if chance <= 0:
                        self.fail_queue.put(fail_node)
                        worker.clean()
                    else:
                        if chance == self.retry_times:
                            new_cmd = 'sleep ' + str(self.retry_span) + ';' + fail_node.get_cmd()
                            fail_node.set_cmd(new_cmd)
                        logging.debug("COMMAND:" + fail_node.get_cmd())
                        fail_node.set_retry_times(chance - 1)
                        worker.execute(fail_node)

                # 从上游队列请求任务
                if not self.ready_queue.empty():
                    to_exe = self.ready_queue.get()
                    worker.execute(to_exe)

    def check_stop(self):
        is_stop = True
        for worker in self.worker_list:
            if worker.get_cur_node():
                is_stop = False
        return is_stop

    def execute(self):
        self.all_node_to_wait()
        self.wait_to_ready()
        while True:
            self.worker_period()
            if self.check_stop():
                if self.fail_queue.empty() and self.ready_queue.empty() and not self.wait_list:
                    self.show_queues()
                    return 0
                else:
                    self.show_queues()
                    return 1
            self.show_queues()
            time.sleep(1)

    def show_queues(self):
        logging.info('-' * 32)
        logging.info("WaitList:")
        logging.info([x.get_name() for x in self.wait_list])
        logging.info("ReadyQ:")
        tmp_list = []
        for i in range(self.ready_queue.qsize()):
            node = self.ready_queue.get()
            name = node.get_name()
            tmp_list.append(name)
            self.ready_queue.put(node)
        logging.info(tmp_list)
        logging.info("ExecuteQ:")
        tmp_list = []
        for worker in self.worker_list:
            node = worker.get_cur_node()
            if node:
                name = node.get_name()
                tmp_list.append(name)
        logging.info(tmp_list)
        logging.info("SuccessQ:")
        tmp_list = []
        for i in range(self.success_queue.qsize()):
            node = self.success_queue.get()
            name = node.get_name()
            tmp_list.append(name)
            self.success_queue.put(node)
        logging.info(tmp_list)
        logging.info("FailedQ:")
        tmp_list = []
        for i in range(self.fail_queue.qsize()):
            node = self.fail_queue.get()
            name = node.get_name()
            tmp_list.append(name)
            self.fail_queue.put(node)
        logging.info(tmp_list)
        logging.info('-' * 32)


def main():
    logging.basicConfig(filename="test.log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                        datefmt="%d-%M-%Y %H:%M:%S", level=logging.DEBUG)
    d = ExeDag('test.conf', 'commands.txt', 3)
    d.set_retry_span(5)
    d.show_all_deps()
    retcode = d.execute()
    print(retcode)


if __name__ == "__main__":
    main()
