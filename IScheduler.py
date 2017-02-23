import BaseThread
import Queue
import json
import time

from MPI_Wrapper import Tags

def MSG_wrapper(**kwd):
    return json.dumps(kwd)

class Policy:
    """
    Collection of policy values. Empty by default.
    """
    ## False => ignore failed tasks and continue running; True => redo the failed tasks
    REDO_IF_FAILED_TASKS = False

class IScheduler(BaseThread):
    def __int__(self, master, appmgr):
        BaseThread.__init__(self, name=self.__class__.__name__)
        self.master = master
        self.appmgr = appmgr
        self.task_todo_Queue = Queue.Queue()
        #self.task_unschedule_queue = Queue.Queue()
        policy = Policy()

    def initialize(self):
        pass

    def has_more_work(self):
        """
        Return ture if current app has more work( when the number of works of app is larger than sum of workers' capacities)
        :return: bool
        """
        pass


    def worker_initialize(self, w_entry):
        """
        called by Master when a worker agent successfully initialized the worker, (maybe check the init_output)
        when the method returns, the worker can be marked as ready
        :param w_entry:
        :return:
        """
        raise NotImplementedError

    def worker_removed(self, w_entry):
        """
        called by Master when a worker has been removed from worker Regitry list (lost. terminated or other reason)
        :param w_entry:
        :return:
        """
        pass

    def task_failed(self,task):
        """
        called when tasks completed with failure
        :param task:
        :return:
        """
        raise NotImplementedError

    def task_completed(self, task):
        """
        this method is called when task completed ok.
        :param task:  Task
        :return:
        """
        raise NotImplementedError

    def task_unschedule(self, tasks):
        """
        called when tasks have been unschedule. tasks that have not been started or that are not completed
        :param tasks:
        :return:
        """

    def run(self):
        raise NotImplementedError

class SimpleScheduler(IScheduler):
    policy = Policy()
    def __init__(self, master, appmgr):
        IScheduler.__init__(master, appmgr)
        self.completed_tasks_queue = Queue.Queue()
        self.processing = False
        self.current_app = appmgr.current_app()[1]
        for t in self.current_app.task_list:
            self.task_todo_Queue.put(self.current_app.task_list[t])

    def worker_removed(self, w_entry):
        for v in w_entry.scheduled_tasks.values():
            self.task_todo_Queue.put_nowait(v)

    def has_more_work(self):
        return not self.task_todo_Queue.empty()

    def worker_initialize(self, w_entry):
        if self.current_app.init_boot:
            send_str = MSG_wrapper(app_ini_boot=self.current_app.init_boot, app_ini_data=self.current_app.init_data,
                               res_dir='/home/cc/zhaobq')
            self.master.server.send_str(send_str, len(send_str), w_entry.w_uuid, Tags.APP_INI)
        else:           #if no init boot, send empty string
            send_str = MSG_wrapper(app_ini_boot="", app_ini_data="",res_dir="")
            self.master.server.send_string(send_str,len(send_str), w_entry.w_uuid, Tags.APP_INI)

    def task_failed(self,task):
        if self.policy.REDO_IF_FAILED_TASKS:
            self.task_unschedule(task)
        else:
            self.task_completed(task)

    def task_completed(self, task):
        #self.completed_tasks.put(task)
        self.appmgr.task_done(self.current_app.app_id, task)

    def task_unschedule(self, tasks):
        for t in tasks:
            self.task_todo_Queue.put(t)

    def run(self):
        """
        1. split application into tasks
        2. initialize worker
        3. assign tasks
        :return:
        """
        self.processing = True
        #TODO split application
        # initialize worker
        try:
            self.master.worker_registry.lock.require()
            for w in self.master.worker_registry.get_worker_list():
                if not w.initialized and not w.current_app:
                    w.current_app = self.current_app
                    self.worker_initialize(w)
        finally:
            self.master.worker_registry.lock.release()

        task_num = 0

        while not self.get_stop_flag():
        #3. assign tasks
            #TODO rewrite schedule tasks
            if self.has_more_work():
                # schedule tasks to initialized workers
                for w, r in self.master.worker_registry.get_aviliable_worker(True):
                    tasks = []
                    try:
                        for i in range(r):
                            tasks.append(self.task_todo_Queue.get_nowait())
                    except Queue.Empty:
                        break
                    if tasks:
                        self.master.schedule(w.w_uuid, tasks)
                    else:
                        break
            # monitor task complete status
            try:
                # while True:
                t = self.completed_tasks.get()
                self.appmgr.task_done(self.current_app, t.tid)
                task_num += 1
                # TODO logging
                if len(self.appmgr.applist[self.current_app].task_list) == task_num:
                    break
            except Queue.Empty:
                pass

            time.sleep(0.1)
        self.appmgr.finilize(self.current_app.app_id)
        # TODO logging app finial
        self.processing = False