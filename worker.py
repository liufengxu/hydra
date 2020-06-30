# -*- coding: utf-8 -*-
import subprocess
import logging

FREE = 0
BUSY = 1

# logging.basicConfig(filename="test.log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
#                     datefmt="%d-%m-%Y %H:%M:%S", level=logging.INFO)


class ShellWorker(object):
    def __init__(self, name):
        self.name = name
        self.task = None
        self.status = FREE
        self.is_success = True
        self.cur_node = None

    def execute(self, node):
        cmd = node.get_cmd()
        self.cur_node = node
        logging.info("To execute cmd:" + cmd)
        self.task = subprocess.Popen(cmd, shell=True)
        self.status = BUSY

    def clean(self):
        self.cur_node = None

    def check_status(self):
        if self.task:
            if self.task.poll() is not None:
                self.status = FREE
                if self.task.poll() == 0:
                    self.is_success = True
                else:
                    self.is_success = False

    def get_status(self):
        return self.status

    def get_is_success(self):
        return self.is_success

    def get_cur_node(self):
        return self.cur_node

    def get_name(self):
        return self.name
