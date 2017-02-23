import IScheduler
from IApplicationManager import SimpleApplicationMgr
import Task
from MPI_Wrapper import Tags
from MPI_Wrapper import Server
import WorkerRegistry
from BaseThread import BaseThread
import IRecv_Module as IM

import logger
import time
import json
import Queue

WORKER_NUM = 1
CONTROL_DELAY = 2 # the interval that controlThread scan worker registry

control_log = logger.getLogger('ControlLog')
log = logger.getLogger('Master')
def MSG_wrapper(**kwd):
    return json.dumps(kwd)

class ControlThread(BaseThread):
    """
    monitor the worker registry to manage worker
    """
    def __init__(self, master):
        BaseThread.__init__(self, 'ControlThread')
        self.master = master
        self.processing = False

    def run(self):
        time_start = time.time()
        control_log.info('Control Thread start...')
        while not self.get_stop_flag():
            try:
                for wid in self.master.worker_registry:
                    w = self.master.worker_registry.get(wid)
                    try:
                        w.alive_lock.require()
                        if w.alive and w.lost():
                            # lost worker
                            control_log.warning('lost worker: %d',wid)
                            self.master.remove_worker(wid)
                            continue
                        if w.alive:
                            if w.processing_task:
                                w.idle_time = 0
                            else:
                                if w.idle_time == 0:
                                    w.idle_time = time.time()
                            if w.idle_timeout():
                                # idle timeout, worker will be removed
                                control_log.warning('worker %d idle too long and will be removed', wid)
                                self.master.remove_worker(wid)
                    finally:
                        w.alive_lock.release()
            finally:
                pass

            time.sleep(CONTROL_DELAY)

    def activateProcessing(self):
        self.processing = True


class IMasterController:
    """
        interface used by Scheduler to control task scheduling
        """

    def schedule(self, wid, task):
        """
        schedule task to be consumed by the worker
        If worker can not be scheduled tasks , then call Scheduler task_unschedule()
        :param wid: worker id
        :param task: task
        :return:
        """
        raise NotImplementedError

    def unschedule(self, wid, tasks=None):
        """
        Release scheduled tasks from the worker, when tasks = None ,release all tasks on worker
        Sucessfully unscheduled tasks will be reported via the tasks_unscheduled() callback on the task manager.
        :param wid: worker id
        :return: list of successfully unscheduld tasks
        """
        pass

    def remove_worker(self, wid):
        """
        Remove worker from the pool and release all unscheduled tasks(all processing tasks will be declared lost)
        :param worker: worker id
        :return:
        """
        raise NotImplementedError

class Master(IMasterController):
    def __init__(self, applications=[], svc_name='TEST'):
        self.svc_name = svc_name
        # worker registery
        self.worker_registry = WorkerRegistry.WorkerRegisty()
        # task scheduler
        self.task_scheduler = None

        self.recv_buffer = IM.IRecv_buffer()

        self.control_thread = ControlThread(self)

        self.applications = applications # to do applications list

        self.__tid = 1
        self.__wid = 1

        self.server = Server(self.recv_buffer, self.svc_name)
        self.server.initialize(svc_name)
        self.server.run()
        log.info('master start server with service_name=%s',self.svc_name)

        self.stop = False

    def schedule(self, w_uuid, tasks):
        for t in tasks:
            w = self.worker_registry.get_by_uuid(w_uuid)
            w.worker_status = WorkerRegistry.WorkerStatus.RUNNING
            w.scheduled_tasks.append(t.tid)
            send_str = MSG_wrapper(tid=t.tid, task_boot=t.task_boot, task_data=t.task_data, res_dir=t.res_dir)
            self.server.send_string(send_str, len(send_str), w_uuid, Tags.TASK_ADD)

    def remove_worker(self, wid):
        self.task_scheduler.worker_removed(self.worker_registry.get(wid))
        self.worker_registry.remove(wid)

    def register(self, w_uuid, capacity=10):
        worker = self.worker_registry.add_worker(w_uuid,capacity)
        if not worker:
            log.warning('the uuid=%s of worker has already registered', w_uuid)
        else:
            self.server.send_int(worker.wid, 1, w_uuid, Tags.MPI_REGISTY_ACK)

    def startProcessing(self):

        self.task_scheduler = IScheduler.SimpleScheduler(self,SimpleApplicationMgr(applications=self.applications))
        self.task_scheduler.start()
        self.control_thread.start()
        # handle received message
        while not self.stop:
            if not self.recv_buffer.empty():
                msg = self.recv_buffer.get()
                if msg.tag == -1:
                    continue

                if msg.tag == Tags.MPI_REGISTY:
                    self.register(msg.ibuf)

                # worker ask for app_ini
                elif msg.tag == Tags.APP_INI_ASK:
                    wid = msg.ibuf

                    init_boot, init_data = self.appmgr.get_app_init(wid)
                    appid, send_str = MSG_wrapper(app_init_boot=init_boot, app_init_data=init_data,
                                                  res_dir='/home/cc/zhaobq')
                    w = self.worker_registry.get(wid)
                    w.current_app = appid
                    self.server.send_string(send_str, len(send_str), w.w_uuid, Tags.APP_INI)

                # worker finish app_ini
                elif msg.tag == Tags.APP_INI:
                    # worker init success or fail
                    recv_dict = eval(json.loads(msg.sbuf))
                    if 'error' in recv_dict:
                        # worker init error TODO stop worker or reassign init_task?
                        log.warning('worker initialized failed')
                        pass
                    else:
                        if self.appmgr.check_init_res(recv_dict['wid'], recv_dict['res_dir']):
                            w = self.worker_registry.get(recv_dict['wid'])
                            try:
                                w.alive_lock.require()
                                w.initialized = True
                                w.worker_status = WorkerRegistry.WorkerStatus.INITILAZED
                            except:
                                pass
                            finally:
                                w.alive_lock.release()
                        else:  # init result error
                            # TODO reassign init_task?
                            pass

                elif msg.tag == Tags.TASK_FIN:
                    recv_dict = eval(json.loads(msg.sbuf))
                    # wid, tid, time_start, time_fin, status
                    w = self.worker_registry.get(recv_dict['wid'])
                    try:
                        w.alive_lock.require()
                        t = self.appmgr.get_task(w.current_app, recv_dict['tid'])
                        if recv_dict['status'] == TaskStatus.COMPLETED:
                            t.status = TaskStatus.COMPLETED
                            # TODO add task other details
                            self.task_scheduler.task_completed(t)
                            del (w.scheduled_tasks[recv_dict['tid']])
                        elif recv_dict['status'] == TaskStatus.FAILED:
                            t.status = TaskStatus.FAILED
                            self.task_scheduler.task_failed(t)
                            del (w.scheduled_tasks[recv_dict['tid']])
                        else:
                            pass
                    finally:
                        w.alive_lock.release()
                elif msg.tag == Tags.APP_FIN:
                    recv_dict = eval(json.loads(msg.sbuf))
                    if self.task_scheduler.has_more_work():
                        # TODO schedule more work
                        pass
                    else:
                        fin_boot, fin_data = self.appmgr.get_app_fin(recv_dict['wid'])
                        send_str = MSG_wrapper(app_fin_boot=fin_boot, app_fin_data=fin_data)
                        self.server.send_string(send_str, len(send_str),
                                                self.worker_registry.get(recv_dict['wid']).w_uuid, Tags.APP_FIN)
                elif msg.tag == Tags.MPI_PING:
                    w = self.worker_registry.get(msg.ibuf)
                    try:
                        w.alive_lock.require()
                        w.last_contact_time = time.time()
                    finally:
                        w.alive_lock.release()