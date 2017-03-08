log = None #will be set in Master

import time
import threading
import traceback

LOST_WORKER_TIMEOUT = 10 # lost worker when overhead this thrashold
IDLE_WORKER_TIMEOUT = 100



class WorkerStatus:
    NEW = -1
    INITILAZED = 0
    IDLE = 1
    RUNNING = 2
    ERROR = 3
    LOST = 4
    COMPELETE = 5
    Finalizing=6

class WorkerEntry:
    """
    contain worker information and task queue
    """
    def __init__(self, wid, w_uuid, max_capacity):
        self.wid = wid
        self.w_uuid = w_uuid
        self.registration_time = time.time()
        self.last_contact_time = self.registration_time
        self.idle_time = 0

        self.max_capacity = max_capacity
        self.assigned = 0

        self.worker_status= WorkerStatus.NEW

        self.initialized = False

        #self.processing_task = None
        #self.current_app = None
        #self.scheduled_tasks = {}

        self.alive = True
        self.alive_lock = threading.RLock()

        self.init_output=None
        self.fin_output=None

    def capacity(self):
        return self.max_capacity-self.assigned

    def lost(self):
        return time.time()-self.last_contact_time > LOST_WORKER_TIMEOUT

    def getStatus(self):
        return self.worker_status

    def idle_timeout(self):
        return self.idle_time and IDLE_WORKER_TIMEOUT and time.time()-self.idle_time > IDLE_WORKER_TIMEOUT

    def initial(self):
        self.initialized = True
        self.worker_status = WorkerStatus.INITILAZED

#    def getStatusReport(self):
#       return "wid=%d alive = %d registered %s last_contact %s (%f seconds ago)\n" % \
#               (self.wid, self.alive, self.registration_time, \
#                self.last_contact_time, \
#                time.time() - self.last_contact_time)

class WorkerRegisty:
    def __init__(self):
        self.__all_workers={}           # w_id:registryEntry
        self.__all_workers_uuid={}      # w_uuid:wid
        self.last_wid= 0
        self.lock = threading.RLock()

        self.__alive_workers = {}       # w_uuid:wid

    def size(self):
        return len(self.__all_workers)

    def add_worker(self, w_uuid, max_capacity):
        self.lock.acquire()
        try:
            if self.__alive_workers.has_key(w_uuid):
                wid = self.__all_workers_uuid[w_uuid]
                log.warning('worker already registered: wid=%d, worker_uuid=%s',wid,w_uuid)
                return None
            else:
                self.last_wid+=1
                newid = self.last_wid
                w = WorkerEntry(newid ,w_uuid, max_capacity)
                self.__all_workers[newid] = w
                self.__all_workers_uuid[w_uuid] = newid
                self.__alive_workers[w_uuid]=newid
                log.info('new worker registered: wid=%d, worker_uuid=%s',newid, w_uuid)
            #self.lock.release()
            return w
        except:
            # logging
            log.error('[WorkerRegistry]: Error occurs when adding worker, msg=%s', traceback.format_exc())
            pass
        finally:
            self.lock.release()

    def remove(self,wid):
        try:
            self.lock.acquire()
            try:
                w_uuid = self.__all_workers[wid].w_uuid
            except KeyError:
                log.warning('attempt to remove not registered worker: wid=%d', wid)
                pass
            else:
                log.info('worker removed: wid=%d',wid)
                self.__all_workers[wid].alive = False
                try:
                    del(self.__all_workers[wid])
                    del(self.__all_workers_uuid[w_uuid])
                    del(self.__alive_workers[w_uuid])
                except KeyError:
                    log.warning('[WorkerRegistry]: can not find worker when remove worker=%d, uuid=%s',wid,w_uuid)
        finally:
            self.lock.release()

    def get(self, wid):
        return self.__all_workers[wid]

    def get_by_uuid(self, w_uuid):
        return self.get(self.__all_workers_uuid[w_uuid])

    def get_worker_list(self):
        return self.__all_workers.values()

    def get_availiable_worker_list(self):
        """
        :return:the list of availiable worker
        """
        availiable_list = []
        for w_uuid in self.__alive_workers:
            w_entry = self.get_by_uuid(w_uuid)
            if w_entry.initialized and w_entry.assigned < w_entry.max_capacity:
                availiable_list.append(w_entry)
        return availiable_list

    def get_aviliable_worker(self, room=False):
    #:param room:
    #:return: room=>true, return wid:room to be assigned
        for w_uuid in self.__alive_workers:
            wentry = self.get_by_uuid(w_uuid)
            if wentry.initialized and wentry.assigned < wentry.max_capacity:
                if room:
                    return (wentry, wentry.max_capacity - wentry.assigned )
                else:
                    return wentry
        if room:
            return (None,-1)
        else:
            return None

    def __iter__(self):
        return self.__all_workers.copy().__iter__()