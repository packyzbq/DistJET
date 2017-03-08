import logger
import Application
import Queue

log = logger.getLogger('AppMgr')
class IApplicationMgr:
    def __init__(self,applications):
        self.applist = {}  # id: app
        self.current_app_id = 0
        self.task_queue = Queue.Queue()
        index = 0
        for app in applications:
            self.applist[index] = app
            index+=1
        self.load_app_tasks(self.applist[self.current_app_id])

    def load_app_tasks(self, app):
        # split app into tasks and put to the task_todo_queue
        app.create_tasks()
        for task in app.task_list.values():
            self.task_queue.put_nowait(task)
            log.info('load task: id=%d', task.tid)

    def get_current_appid(self):
        return self.current_app_id

    def current_app(self):
        return self.current_app_id,self.applist[self.current_app_id]

    def next_app(self):
        raise NotImplementedError

    def task_done(self, app, tid):
        raise NotImplementedError



class SimpleApplicationMgr(IApplicationMgr):

    def has_more_work(self):
        return self.task_queue.empty()

    def next_app(self):
        if len(self.applist) - self.current_app_id > 1:
            self.current_app_id+=1
            return self.applist[self.current_app_id]
        else:
            return None

    def task_done(self, app, tid):
        app.task_done(tid)