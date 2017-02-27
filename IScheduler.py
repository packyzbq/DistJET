import Task
import Policy
import BaseThread
import logger
import Queue
import json
import time

from MPI_Wrapper import Tags

log = logger.getLogger('TaskScheduler')

def MSG_wrapper(**kwd):
    return json.dumps(kwd)


class IScheduler(BaseThread):
    def __int__(self, master, appmgr):
        BaseThread.__init__(self, name=self.__class__.__name__)
        self.master = master
        self.appmgr = appmgr
        self.task_todo_Queue = Queue.Queue()
        self.completed_Queue = Queue.Queue()
        #self.task_unschedule_queue = Queue.Queue()
        self.policy = Policy()

    def initialize(self):
        pass

    def set_running_task(self, tid):
        self.appmgr.current_app().task_list[tid].status = Task.TaskStatus.PROCESSING

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

    def task_failed(self,tid):
        """
        called when tasks completed with failure
        :param task:
        :return:
        """
        raise NotImplementedError

    def task_completed(self, tid, time_start, time_finish):
        """
        this method is called when task completed ok.
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
    #policy = Policy()
    def __init__(self, master, appmgr):
        IScheduler.__init__(master, appmgr)
        #self.completed_tasks_queue = Queue.Queue()
        self.processing = False
        self.current_app = self.appmgr.current_app()[1]
        self.scheduled_task_queue = {}                  # record tasks matches worker
        for t in self.current_app.task_list:
            self.task_todo_Queue.put(self.current_app.task_list[t])

    def worker_removed(self, w_entry):
        q = self.scheduled_task_queue[w_entry.wid]
        while not q.empty():
            self.task_todo_Queue.put_nowait(q.get())

    def has_more_work(self):
        return not self.task_todo_Queue.empty()

    def worker_initialize(self, w_entry):
        if self.current_app.init_boot:
            send_str = MSG_wrapper(app_ini_boot=self.current_app.init_boot, app_ini_data=self.current_app.init_data,
                               res_dir=self.current_app.res_dir)
            self.master.server.send_str(send_str, len(send_str), w_entry.w_uuid, Tags.APP_INI)
        else:           #if no init boot, send empty string
            send_str = MSG_wrapper(app_ini_boot="", app_ini_data="",res_dir="")
            self.master.server.send_string(send_str,len(send_str), w_entry.w_uuid, Tags.APP_INI)

    def task_failed(self,tid):
        task = self.current_app.get_task_by_id(tid)
        if self.policy.REDO_IF_FAILED_TASKS and len(task.history) < self.policy.REDO_LIMITS:
            log.info('TaskScheduler: task=%d fail, waiting for reassign', tid)
            self.task_unschedule(task)
        else:
            log.info('TaskScheduler: task=%d fail, ignored')
            task.fail()
            self.completed_Queue.put_nowait(task)

    def task_completed(self, tid, time_start, time_finish):
        task = self.current_app.get_task_by_id(tid)
        task.complete(time_start, time_finish)
        self.completed_tasks.put(task)
        log.info('TaskScheduler: task=%d complete', tid)

    def task_unschedule(self, tasks):
        for t in tasks:
            self.task_todo_Queue.put(t)

    def run(self):
        """
        1. split application into tasks
        2. check initialize worker
        3. assign tasks
        :return:
        """
        self.processing = True
        #TODO split application
        # initialize worker
        """
        try:
            self.master.worker_registry.lock.require()
            for w in self.master.worker_registry.get_worker_list():
                if not w.initialized and not w.current_app:
                    w.current_app = self.current_app
                    self.worker_initialize(w)
        finally:
            self.master.worker_registry.lock.release()
        """
        task_num = 0

        while not self.get_stop_flag():
        #3. assign tasks
            if self.has_more_work():
                # schedule tasks to initialized workers
                availiable_list = self.master.worker_registry.get_availiable_worker_list()
                if availiable_list:
                    # if list is not empty, then assign task
                    for w in availiable_list:
                        tmptask = self.task_todo_Queue.get_nowait()
                        try:
                            if not self.scheduled_task_queue.has_key(w.wid):
                                self.scheduled_task_queue[w.wid] = Queue.Queue()
                            self.scheduled_task_queue[w.wid].put(tmptask)
                            self.master.schedule(w.w_uuid, tmptask)
                        except Queue.Empty:
                            break
                    #TODO no task assigned worker idle? or quit?
            # monitor task complete status
                # while True:
            if not self.completed_Queue.empty():
                t = self.completed_tasks.get()
                self.appmgr.task_done(self.current_app, t.tid)
                task_num += 1
                # TODO logging
                if len(self.appmgr.applist[self.current_app].task_list) == task_num:
                    break

            time.sleep(0.1)
        self.appmgr.finilize(self.current_app.app_id)
        # TODO logging app finial
        self.processing = False