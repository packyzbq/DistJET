import Queue
import json
import subprocess
import threading
import time
import ConfigParser
import sys

import IRecv_Module as IM

import logger
from BaseThread import BaseThread
from MPI_Wrapper import Client
from MPI_Wrapper import Tags
from Policy import Policy
from Task import SampleTask
from Task import TaskStatus
from WorkerRegistry import WorkerStatus

policy = Policy()

log = logger.getLogger('WorkerAgent')

class HeartbeatThread(BaseThread):
    """
    ping to master to update status
    """
    def __init__(self, client, worker_agent):
        BaseThread.__init__(self,name='HeartbeatThread')
        self._client = client
        self.worker_agent = worker_agent

    def run(self):
        try:
            last_ping_time = time.time()
            while not self.get_stop_flag():
                if last_ping_time and (time.time()-last_ping_time) >= policy.PING_DELAY:
                    #ping server
                    self._client.send_int(self.worker_agent.wid, 1, 0, Tags.MPI_PING)
                    self.worker_agent.log.info("HeartBeat: ping master...")
                    #self._client.ping()
                    send_str = self.worker_agent.MSG_wrapper(wid=self.worker_agent.wid, tid=self.worker_agent.running_task)
                    self._client.send_string(send_str, len(send_str), 0, Tags.TASK_SYNC)
                    last_ping_time = time.time()
                    self.worker_agent.log.debug('HeartBeatThread: time=%s, Ping master with running task:%d',last_ping_time,self.worker_agent.running_task)
                else:
                    time.sleep(1)
        except Exception:

            self.worker_agent.log.error('HeartBeatThread: unkown error, thread stop')

        self.stop()
        self.worker_agent.stop()

class WorkerAgent(BaseThread):
    """
    agent
    """
    def __init__(self, svcname, capacity=5):
        BaseThread.__init__(self, name='WorkerAgent')
        self.log = None
        self.recv_buffer = IM.IRecv_buffer()
        import uuid as uuid_mod
        self.uuid = str(uuid_mod.uuid4())
        self.client = Client(self.recv_buffer, svcname, self.uuid)
        self.client.initial()
        self.wid = None
        #self.appid = None # the app that are running
        self.capacity = capacity                   # can change
        self.task_queue=Queue.Queue(maxsize=self.capacity) #~need lock~ thread safe
        self.task_completed_queue = Queue.Queue()

        self.app_ini_task = None
        self.app_ini_task_lock = threading.RLock()
        self.app_fin_task = None
        self.app_fin_task_lock = threading.RLock()

        self.task_sync_flag = False
        self.task_sync_lock = threading.RLock()

        self.heartbeat_thread=None
        self.cond = threading.Condition()
        self.worker=Worker(self, self.cond)
        self.running_task = None
        #self.worker_status = WorkerStatus.NEW

        #self.app_init_boot = None
        #self.app_init_data = None
        #self.app_fin_boot = None


        # init_data finalize_bash result dir can be store in Task object
        self.register_flag = False
        self.register_time = None

        self.initialized = False
        self.finalized = False

    def register(self):
        self.worker.start()
        ret = self.client.send_string(self.uuid, len(self.uuid), 0, Tags.MPI_REGISTY)
        if ret != 0:
            #TODO add error handler
            pass
        #register to master, take down register info
        #log.info("WorkerAgent: register to master...")
        self.register_time = time.time()

    def MSG_wrapper(self, **kwd):
        return json.dumps(kwd)

    def run(self):
        # use while to check receive buffer or Client buffer
        self.client.run()
        self.register()
        # ensure the worker is registered and initialed
        while True:
            #already register and inistialize
            if self.register_flag and self.initialized:
                break

            if not self.recv_buffer.empty():
                msg_t = self.recv_buffer.get()
                if msg_t.tag == -1:
                    continue
                # comfirm worker is registered
                if not self.register_flag:
                    if msg_t.tag == Tags.MPI_REGISTY_ACK:
                        self.wid = msg_t.ibuf
                        if msg_t.ibuf > 0:
                            # register successfully
                            log.info("WorkerAgent: Register successfully, my worker_id = %d, create new log for worker", msg_t.ibuf)
                            self.log = logger.getLogger('Worker_'+str(self.wid))
                            self.heartbeat_thread = HeartbeatThread(self.client, self)
                            self.heartbeat_thread.start()
                            self.register_flag = True
                            # ask for api_ini
                            self.client.send_int(self.wid, 1, 0, Tags.APP_INI_ASK)
                            continue
                        else:
                            # register fail
                            log.error("WorkerAgent: Register failed, stop workeragent, uuid=%s", self.uuid)
                            self.client.stop()
                            exit()
                    else:
                        continue
                # confirm worker is initialed
                if not self.initialized:
                    if msg_t.tag == Tags.APP_INI:
                        task_info = eval(json.loads(msg_t.sbuf))
                        #assert task_info.has_key('app_ini_boot') and task_info.has_key('app_ini_data') and task_info.has_key('res_dir')
                        self.log.debug("WorkerAgent: Receive API_INI msg = %s", msg_t.sbuf)
                        self.app_ini_task_lock.acquire()
                        self.app_ini_task = SampleTask(0, task_info['app_ini_boot'], task_info['app_ini_data'], task_info['res_dir'])
                        self.app_ini_task_lock.release()
                        #self.task_queue.put(tmp_task)
                        #wake worker
                        if self.worker.get_status() == WorkerStatus.NEW:
                            self.cond.acquire()
                            self.cond.notify()
                            self.cond.release()

                    else:
                        continue
            else:
                continue
        # message handle
        while not self.get_stop_flag():

            # single task finish ,notify master
            self.task_sync_lock.acquire()
            if self.task_sync_flag:
                self.task_sync_flag = False

                while not self.task_completed_queue.empty():
                    tmp_task= self.task_completed_queue.get()
                    send_str = self.MSG_wrapper(wid=self.wid, tid=tmp_task.tid, time_start=tmp_task.time_start, time_fin=tmp_task.time_finish, status=tmp_task.task_status)
                    self.client.send_string(send_str, len(send_str), 0, Tags.TASK_FIN)
            self.task_sync_lock.release()
            # handle msg from master
            if not self.recv_buffer.empty():
                msg_t = self.recv_buffer.get()

                # recv register ack then ask for app_ini
                """if msg_t.tag == Tags.MPI_REGISTY_ACK:
                    self.wid = msg_t.ibuf
                    self.log.info('WorkerAgent: Receive register ack: wid=%d',self.wid)
                    self.client.send_int(self.wid,1,self.uuid,Tags.APP_INI_ASK)
                """
                # recv app_ini, do worker initialization
                if msg_t.tag == Tags.APP_INI:
                    #TODO consider if not a complete command
                    self.log.debug('WorkerAgent: receive APP_INI message')
                    comm_dict = json.loads(msg_t.sbuf)
                    self.appid = comm_dict['appid']
                    self.app_ini_task_lock.acquire()
                    self.app_ini_task = SampleTask(0, comm_dict['app_init_boot'], comm_dict['app_init_data'], comm_dict['res_dir'])
                    self.app_ini_task_lock.release()
                    #wake worker up and do app initialize
                    self.log.debug('WorkerAgent: Initialize worker...')
                    self.cond.acquire()
                    self.cond.notify()
                    self.cond.release()

                elif msg_t.tag == Tags.TASK_ADD:
                    if self.task_queue.qsize() == self.capacity:
                        # add error handler: out of queue bound
                        self.log.error('error to add tasks: out of capacity')
                        # TODO add some feedback to Master?
                    else:
                        comm_dict = json.loads(msg_t.sbuf)
                        task = SampleTask(comm_dict['tid'], comm_dict['task_boot'], comm_dict['task_data'], comm_dict['res_dir'])
                        #task.task_status = TaskStatus.SCHEDULED_HALT
                        self.log.debug('WorkerAgent: add new task=%d into to-do queue, now have %d task to be performed',task.tid, self.task_queue.qsize())
                        self.task_queue.put_nowait(task.tid)
                        #self.task_list[task.tid] = task
                        if self.worker.status == WorkerStatus.IDLE:
                            self.cond.acquire()
                            self.cond.notify()
                            self.cond.release()

                elif msg_t.tag == Tags.TASK_REMOVE:
                    pass
                elif msg_t.tag == Tags.WORKER_STOP:
                    pass
                elif msg_t.tag == Tags.APP_FIN:
                    self.log.debug('WorkerAgent: receive APP_FIN message, msg= %s',msg_t.sbuf)
                    comm_dict = json.loads(msg_t.sbuf)
                    self.app_fin_task_lock.acquire()
                    self.app_fin_task = SampleTask(0, comm_dict['app_fin_boot'], None, None)
                    self.app_fin_task_lock.release()
                    #self.task_queue.put_nowait(task)
                    self.worker.finialize = True
                    if self.worker.get_status() == WorkerStatus.IDLE:
                        self.cond.acquire()
                        self.cond.notify()
                        self.cond.release()
                    #self.worker.work_finalize()
                    #self.worker_status = WorkerStatus.IDLE

                elif msg_t.tag == Tags.TASK_SYNC:
                    # return the running task
                    self.log.debug('WorkerAgent: receive TASK_SYNC message=%s', msg_t.sbuf)
                    send_str = self.MSG_wrapper(wid=self.wid, tid=self.running_task)
                    self.client.send_string(send_str,len(send_str), 0,Tags.TASK_SYNC)



            # ask master for app fin, master may add new tasks or give stop order
            if self.task_queue.empty():
                #self.worker_status = WorkerStatus.IDLE
                while not self.task_completed_queue.empty():
                    tmp_task = self.task_completed_queue.get()
                    send_str = self.MSG_wrapper(wid=self.wid, tid=tmp_task.tid, time_start=tmp_task.time_start,
                                                time_fin=tmp_task.time_finish, status=tmp_task.task_status)
                    self.client.send_string(send_str, len(send_str), 0, Tags.TASK_FIN)
                send_str = self.MSG_wrapper(wid=self.wid, app_id = self.appid)
                self.client.send_string(send_str,len(send_str), 0, Tags.APP_FIN)
                if self.worker.status == WorkerStatus.COMPELETE:
                    #notify worker and stop
                    self.cond.acquire()
                    self.cond.notify()
                    self.cond.release()
                    break


            #TODO monitor the task queue, when less than thrashold, ask for more task
            #TODO considor new Application come
            # loop delay
            time.sleep(0.1)
            #if not RT_PULL_REQUEST:
            #    time.sleep(PULL_REQUEST_DELAY)

        self.stop()

    def stop(self):
        BaseThread.stop()
        self.client.stop()
        if self.heartbeat_thread:
            self.heartbeat_thread.stop()
        #client stop

    def task_done(self, task):
        #self.task_list[task.tid] = task
        try:
            self.task_sync_lock.acquire()
            self.task_sync_flag = True
            self.task_completed_queue.put_nowait(task)
        finally:
            self.task_sync_lock.release()

    def app_ini_done(self):
        if self.task_completed_queue.qsize() > 0:
            task = self.task_completed_queue.get()
            if task.task_status == TaskStatus.COMPLETED:
                self.initialized = True
                send_str = self.MSG_wrapper(wid=self.wid, res_dir=task.res_dir)
                self.client.send_string(send_str, len(send_str), 0, Tags.APP_INI)
            else:
                # init error
                log.warning('WorkerAgent: worker=%d init error', self.wid)
                #self.worker_status = WorkerStatus.IDLE
                send_str = self.MSG_wrapper(wid=self.wid, res_dir=task.res_dir, error='initialize error')
                self.client.send_string(send_str, len(send_str), 0, Tags.APP_INI)
        else:
            #can't find completed task error
            pass

    def app_fin_done(self):
        """
        finialize end, worker will diconnect from master (to be modified)
        :return:
        """
        if self.task_queue.empty() and self.task_completed_queue.qsize() > 0:
            self.task_completed_queue.get()

        self.client.send_int(self.wid, 1, 0, Tags.LOGOUT)


    def remove_task(self, taskid):
        pass

    def add_task(self, taskid , task):
        pass

#    def handler_recv(self, tags, pack):
#        msg = MSG(tags, pack)
#        self.recv_handler.MSGqueue.put_nowait(msg)

class Worker(BaseThread):
    """
    worker
    """
    def __init__(self,workagent, cond):
        BaseThread.__init__(self,"worker")
        self.workagent = workagent
        self.running_task = None

        self.cond = cond

        self.initialized = False
        self.finialize = False
        self.status = WorkerStatus.NEW

    def run(self):
        #check worker agent's task queue, initial app
        while not self.initialized:
            self.status = WorkerStatus.NEW
            self.cond.acquire()
            self.cond.wait()
            self.cond.release()
            self.workagent.app_ini_task_lock.acquire()
            self.work_initial(self.workagent.app_ini_task)
            self.workagent.app_ini_task_lock.release()
            if self.initialized == False:
                continue

        self.workagent.log.info("Worker: Initialized... Ready to running tasks")
        while not self.get_stop_flag():
            self.status = WorkerStatus.RUNNING
            while not self.workagent.task_queue.empty():
                task = self.workagent.task_queue.get()
                self.workagent.running_task = task.tid
                succ = self.do_work(task)
                if not succ:
                    # change TaskStatus logging
                    task.status = TaskStatus.FAILED
                    self.workagent.log.error("Worker: execute task=%d error",task.tid)

                self.workagent.task_completed_queue.put(task)
                self.workagent.task_sync_flag = True

            self.status = WorkerStatus.IDLE
            self.cond.acquire()
            self.cond.wait()
            self.cond.release()
            if self.finialize:
                break

        # do finalize
        self.workagent.app_fin_task_lock.acquire()
        self.work_finalize(self.workagent.app_fin_task)
        self.workagent.app_fin_task_lock.release()
        # sleep or stop
        self.cond.acquire()
        self.cond.wait()
        self.cond.release()


    def do_task(self,task):
        task.time_start = time.time()
        #task.task_status = TaskStatus.PROCESSING
        rc = subprocess.Popen([task.task_boot, task.task_data], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, stderr = rc.communicate()
        rc.wait()
        task.time_finish = time.time()
        # store output into log file
        with open(task.res_dir+'result_'+str(task.tid), 'w+') as resfile:
            resfile.write(stdout)
        if len(stderr) != 0:
            with open(task.res_dir+'error_'+str(task.tid), 'w+') as errfile:
                errfile.write(stderr)
        return len(stderr) == 0
#        if len(stderr) == 0:  # no error
#            self.initialized = True
#            self.status = WorkerStatus.INITILAZED
#            task.task_status = TaskStatus.COMPLETED
#        else:
#            task.task_status = TaskStatus.FAILED
#            self.initialized = False
#

    def work_initial(self, task):
        self.running_task = task
        #do the app init
        if not task.task_boot and not task.task_data:
            task.task_status = TaskStatus.COMPLETED
            self.initialized = True
        else:
            #TODO execuate the bash/.py
            if self.do_task(task):
                self.initialized = True
                self.status = WorkerStatus.INITILAZED
                task.task_status = TaskStatus.COMPLETED
            else:
                task.task_status =TaskStatus.FAILED
                self.status = WorkerStatus.IDLE
                self.initialized = False
        self.workagent.task_completed_queue.put(task)
        self.workagent.app_ini_done()
        #sleep
        if not self.initialized:
            self.cond.acquire()
            self.cond.wait()
            self.cond.release()


    def do_work(self, task):
        self.running_task = task
        return self.do_task(task)

    def work_finalize(self, fin_task):
        self.running_task = fin_task
        if fin_task.task_boot:
            self.do_task(fin_task)
            self.workagent.task_completed_queue.put(fin_task)
        self.status = WorkerStatus.COMPELETE
        self.workagent.app_fin_done()


    def stop(self):
        pass

    def get_status(self):
        return self.status

    def set_status(self, status):
        self.status = status


if __name__ == '__main__':
    script = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read(script)
    app_conf_list = conf.sections()
    svc_name = None
    capacity = 1

    #workspace = conf.get('global', 'workspace')
    svc_name = conf.get('global', 'service_name')
    capacity = conf.get('global','worker_capacity')


    workerAgent = WorkerAgent(svc_name, capacity)
    workerAgent.start()