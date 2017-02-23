import logging
import Application
import Queue


class IApplicationMgr:
    def __init__(self,applications):
        self.applist = {}  #id: app
        self.current_app_id = 0
        self.task_queue = Queue.Queue()
        index = 0
        for app in applications:
            self.applist[index] = app
            index+=1
        self.load_app_tasks(self.applist[self.current_app_id])

    def load_app_tasks(self, app):
        for task in app.task_list.values():
            self.task_queue.put_nowait(task)

    def current_app(self):
        return self.current_app_id,self.applist[self.current_app_id]

    def initialize(self):
        pass

    def finilize(self):
        pass

    def create_task(self):
        """
        create task list
        :return: list of tasks
        """""
        pass

    def task_done(self, task):
        pass

    def has_more_work(self):
        raise NotImplementedError


class SimpleApplicationMgr(IApplicationMgr):

    def has_more_work(self):
        return self.task_queue.empty()

    def has_more_app(self):
        return len(self.applist) - self.current_app_id > 1