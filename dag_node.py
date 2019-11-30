# -*- coding: utf-8 -*-
class TaskNode(object):
    def __init__(self, name):
        self.name = ''
        self.cmd = ':'  # 默认空操作
        self.set_name(name)
        self.deps = set()
        self.retry_times = 0

    def get_name(self):
        return self.name

    def set_name(self, name):
        self.name = name

    def get_deps(self):
        return self.deps

    def add_dep(self, dep):
        self.deps.add(dep)

    def remove_dep(self, dep):
        if dep in self.deps:
            self.deps.remove(dep)

    def get_cmd(self):
        return self.cmd

    def set_cmd(self, cmd):
        self.cmd = cmd

    def get_retry_times(self):
        return self.retry_times

    def set_retry_times(self, times):
        self.retry_times = times
