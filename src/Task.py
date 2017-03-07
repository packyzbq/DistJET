import logging
import time

class TaskStatus:
    """
    tansk status enumeration
    """
    NEW = 0
    INITIALIZED = 1
    PROCESSING = 2
    COMPLETED = 3
    FAILED = 4
    LOST = 5
    #UNSCHEDULED = 6             # to be scheduled
    SCHEDULED_HALT = 7          # have scheduled, to be performed

class Task:
    """
    task to be scheduled for worker to execute
    """
    def __init__(self, tid):
        self.tid = tid
        self.status = TaskStatus.NEW

        self.history = [TaskDetail()]

        self.task_boot = []
        self.task_data = None

        self.res_dir = None

    def initial(self, work_script=None, data = None, res_dir="./"):
        self.task_boot = work_script
        self.res_dir = res_dir
        self.task_data = data
        self.status = TaskStatus.INITIALIZED

    def status(self):
        return self.status

    def fail(self):
        self.status = TaskStatus.FAILED

    def complete(self, time_start, time_finish):
        self.status = TaskStatus.COMPLETED
        self.details().update(time_start,time_finish)

    def assign(self, wid):
        if not self.status is TaskStatus.NEW:
            try:
                assert (self.status not in [TaskStatus.FAILED, TaskStatus.LOST])
            except:
                print("@Task: task is in wrong status when assign")
            self.history.append(TaskDetail())
        self.details().assign(wid)
        self.status = TaskStatus.SCHEDULED_HALT

    def details(self):
        return self.history[-1]

class TaskDetail:
    """
    details about task status for a single execution attempt
    """
    def __int__(self):
        self.assigned_wid = -1
        #self.result_dir = None
        self.time_start = 0
        self.time_exec = 0
        self.time_finish = 0
        self.time_scheduled = 0
        #self.result = TaskStatus.NEW

        self.error = None # store error code

    def assign(self, wid):
        try:
            assert(wid >0)
            self.assigned_wid = wid
            assert (self.assigned_wid != -1)
            self.time_scheduled = time.time()
            return True
        except:
            return False


    def update(self, time_start, time_finish):
        self.time_start = time_start
        self.time_finish = time_finish



class SampleTask:
    """
    used for workeragent <-> worker
    """
    def __init__(self, tid, boot, data, resdir):
        self.tid = tid
        self.task_boot=boot
        self.task_data=data
        self.res_dir=resdir
        self.task_status = TaskStatus.NEW