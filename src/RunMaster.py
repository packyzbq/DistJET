import json
import time
import ConfigParser
import sys
import re

import logger

control_log = logger.getLogger('ControlLog')
log = logger.getLogger('Master')

import IRecv_Module as IM

import IScheduler
import WorkerRegistry
WorkerRegistry.log = log
from BaseThread import BaseThread
from IApplicationManager import SimpleApplicationMgr
from MPI_Wrapper import Server
from MPI_Wrapper import Tags
from Task import TaskStatus
from Policy import Policy

WORKER_NUM = 1
CONTROL_DELAY = 2 # the interval that controlThread scan worker registry


policy = Policy()
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
                        w.alive_lock.acquire()
                        if w.alive and w.lost():
                            # lost worker
                            control_log.warning('lost worker: %d',wid)
                            self.master.remove_worker(wid)
                            continue
                        if w.alive:
                            if w.worker_status==WorkerRegistry.WorkerStatus.RUNNING:
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
        self.server.initialize()
        #self.server.run()
        log.info('Master: start server with service_name=%s',self.svc_name)

        self.__stop = False

    def schedule(self, w_uuid, tasks):
        for t in tasks:
            w = self.worker_registry.get_by_uuid(w_uuid)
            w.worker_status = WorkerRegistry.WorkerStatus.RUNNING
            #w.scheduled_tasks.append(t.tid)
            w.assigned+=1
            log.info('Master: assign task=%d to worker=%d', t.tid, w.wid)
            if t.details().assign(w.wid):
                send_str = MSG_wrapper(tid=t.tid, task_boot=t.task_boot, task_data=t.task_data, task_args=t.task_args, task_flag = t.task_flag, res_dir=t.res_dir)
                self.server.send_string(send_str, len(send_str), w_uuid, Tags.TASK_ADD)
                return True
            else:
                log.error("Master: assign error")
                return False

    def remove_worker(self, wid):
        self.task_scheduler.worker_removed(self.worker_registry.get(wid))
        self.worker_registry.remove(wid)

    def register(self, w_uuid, capacity=10):
        worker = self.worker_registry.add_worker(w_uuid,capacity)
        if not worker:
            log.warning('Master: the uuid=%s of worker has already registered', w_uuid)
        else:
            send_str = MSG_wrapper(wid=worker.wid)
            self.server.send_string(send_str, len(send_str), w_uuid, Tags.MPI_REGISTY_ACK)

    def stop(self):
        self.task_scheduler.join()
        log.info('Master: TaskScheduler has joined')
        self.control_thread.stop()
        self.control_thread.join()
        log.info('Master: Control Thread has joined')
        self.server.stop()
        log.info('Master: Server stoped')
        self.__stop = True

    def startProcessing(self):
        simple_appmgr = SimpleApplicationMgr(applications=self.applications)
        self.task_scheduler = IScheduler.SimpleScheduler(self, simple_appmgr)
        self.task_scheduler.start()
        self.control_thread.start()
        # handle received message
        while not self.__stop:
            if not self.task_scheduler.processing and self.worker_registry.size() == 0:
                log.info('Master have done all applications, ready to stop')
                self.stop()
            if not self.recv_buffer.empty():
                msg = self.recv_buffer.get()
                print('[Python-Master]: Got a msg object, msg=%s' % msg.sbuf)
                if msg.tag == -1:
                    continue
                size = msg.size
                print('[Master_receiver]: get msg=%s, size=%d' % (msg.sbuf, size))
                if msg.tag == Tags.MPI_REGISTY:
                    uuid = msg.sbuf
                    while self.recv_buffer.empty():
                        pass
                    msg = self.recv_buffer.get()
                    recv_dict = json.loads(msg.sbuf[0:msg.size])
                    if msg.tag == Tags.WORKER_INFO and recv_dict['uuid'] == uuid:
                        capacity = recv_dict['capacity']
                        log.info('Master: Receive registry from worker=%s with capacity=%d', uuid, capacity)
                        self.register(uuid, capacity)
                    else:
                        log.error('Master: Worker=%s register error when sync capacity', uuid)

                # worker ask for app_ini
                elif msg.tag == Tags.APP_INI_ASK:
                    wid = int(json.loads(msg.sbuf[0:size])['wid'])
                    self.task_scheduler.worker_initialize(self.worker_registry.get(wid))
                    #init_boot, init_data = self.appmgr.get_app_init(wid)
                    #appid, send_str = MSG_wrapper(app_init_boot=init_boot, app_init_data=init_data,res_dir='/home/cc/zhaobq')
                    #w = self.worker_registry.get(wid)
                    #w.current_app = self.task_scheduler.appmgr.get_current_appid()
                    #self.server.send_string(send_str, len(send_str), w.w_uuid, Tags.APP_INI)
                # worker finish app_ini
                elif msg.tag == Tags.APP_INI:
                    # worker init success or fail
                    recv_dict = json.loads(msg.sbuf[0:size])
                    if 'error' in recv_dict:
                        # worker init error  /stop worker or reassign init_task?
                        log.warning('Master: worker=%d initialized failed', recv_dict['wid'])
                        if policy.REDO_IF_WORKER_INIT_FAIL:
                            log.warning('Master: worker=%d ready to initilization', recv_dict['wid'])
                            pass
                        else:
                            log.warning('Master: worker=%d ignore failed initilization', recv_dict['wid'])
                    else:
                        w = self.worker_registry.get(recv_dict['wid'])
                        try:
                            w.alive_lock.acquire()
                            w.initial()
                        finally:
                            w.alive_lock.release()

                        log.info('Master: worker:%d initial finished', recv_dict['wid'])
                # worker finish task
                elif msg.tag == Tags.TASK_FIN:
                    recv_dict = json.loads(msg.sbuf[0:size])
                    # wid, tid, time_start, time_fin, status
                    if recv_dict['status'] == TaskStatus.COMPLETED:
                        self.task_scheduler.task_completed(recv_dict['wid'],recv_dict['tid'], recv_dict['time_start'],recv_dict['time_fin'])
                    else:
                        self.task_scheduler.task_failed(recv_dict['wid'], recv_dict['tid'])
                    w = self.worker_registry.get(recv_dict['wid'])
                    w.assigned -= 1
                # worker finish app
                # worker finish app
                elif msg.tag == Tags.APP_FIN:
                    recv_dict = json.loads(msg.sbuf[0:size])
                    if self.task_scheduler.has_more_work():
                        # schedule 1 more work
                        log.info('Master: worker=%d ask for app_fin, 1 more task is assigned', recv_dict['wid'])
                        self.task_scheduler.req_more_task(recv_dict['wid'])
                    else:
                        #fin_boot, fin_data = self.task_scheduler.appmgr.get_app_fin(recv_dict['wid'])
                        #send_str = MSG_wrapper(app_fin_boot=fin_boot, app_fin_data=fin_data)
                        #self.server.send_string(send_str, len(send_str), self.worker_registry.get(recv_dict['wid']).w_uuid, Tags.APP_FIN)
                        log.info('Master: worker=%d ask for app_fin, no more task', recv_dict['wid'])
                        self.task_scheduler.worker_fininalize(self.worker_registry.get(recv_dict['wid']))
                elif msg.tag == Tags.MPI_PING:
                    recv_dict = json.loads(msg.sbuf[0:size])
                    w = self.worker_registry.get(int(recv_dict['wid']))
                    try:
                        w.alive_lock.acquire()
                        w.last_contact_time = time.time()
                    finally:
                        w.alive_lock.release()
                # feedback of require for current task
                elif msg.tag == Tags.TASK_SYNC:
                    # receive worker running task
                    recv_dict = json.loads(msg.sbuf[0:size])
                    if recv_dict['tid']:
                        self.task_scheduler.set_running_task(recv_dict['tid'])
                # app done by worker
                elif msg.tag == Tags.LOGOUT:
                    recv_dict = json.loads(msg.sbuf[0:size])
                    w_uuid = self.worker_registry.get(recv_dict['wid']).w_uuid
                    send_str = MSG_wrapper(w='$')
                    self.server.send_string(send_str,len(send_str), w_uuid, Tags.LOGOUT_ACK)

                elif msg.tag == Tags.LOGOUT_ACK:
                    recv_dict = json.loads(msg.sbuf[0:size])
                    self.remove_worker(recv_dict['wid'])




if __name__ == "__main__":
    script = sys.argv[1]

    # read the config script
    conf = ConfigParser.ConfigParser()
    conf.read(script)
    app_conf_list = conf.sections()
    applications = []
    svc_name = None
    from Application import UnitTestApp

    #workspace = conf.get('global', 'workspace')
    svc_name = conf.get('global', 'service_name')
    # MORE

    for item in app_conf_list:
        if item == 'global':
            continue
        app = UnitTestApp()
        app.set_boot(conf.get(item, "boot"))
        app.set_resdir(conf.get(item, "result_dir"))
        app.set_data(conf.get(item, "data"))
        applications.append(app)

    if not svc_name:
        svc_name = "TEST"
    master = Master(applications,svc_name)
    master.startProcessing()