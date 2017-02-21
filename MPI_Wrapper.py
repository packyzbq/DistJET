import Server_Module as SM
import Client_Module as CM
import Queue
#Waring remember to add exception handler: try catch

class Tags:
    MPI_PING = 6

    MPI_REGISTY = 11
    MPI_REGISTY_ACK = 12
    MPI_DISCONNECT = 13
###### NUM <100 for mpich ; NUM>= 100 for python
    WORKER_STOP =100


    TASK_FIN = 110      #w->m   worker notify completed tasks,
    TASK_SYNC = 111     #m<->w   master ask for work info
    APP_INI = 112   #m->w   master schedule app and transfer the init data  (app_ini_boot, app_ini_data, res_dir)
                    #w->m   init result                                     (wid, res_dir)
    APP_INI_ASK = 113   #w->m ask for app ini boot and data
    TASK_ADD = 114      #m->w                                                   (tid, task_boot, task_data, res_dir)
    TASK_REMOVE = 115   #m->w   remove worker task, maybe give it to another worker (tid)
    APP_FIN = 116       #m->w   master tell worker how to finalize
                        #W->M   worker ask for finalize operation
    LOGOUT  = 120

#class Recv_handler(SM.IRecv_handler):
#    def __init__(self):
#        self.MSGqueue = Queue.Queue()
#
#    def handler_recv(self, tags, pack):
#        msg = MSG(tags,pack)
#        self.MSGqueue.put_nowait(msg)

class Server:
    """
    Set up a server using C++ lib
    """
    def __init__(self, recv_buffer, svcname):
        self.server = SM.MPI_Server(recv_buffer, svcname)
    def initial(self):
        ret = self.server.initialize()
        if ret != 0:
            #TODO log init error
            pass

    def send_int(self, int_data, msgsize, dest, tags):
        self.server.send_int(int_data, msgsize, dest, tags)

    def send_string(self, str ,msgsize, dest, tag):
        self.server.send_string(str, msgsize, dest, tag)

    def command_analyze(self, command):
        pass


class Client:
    """
    Set up a client(workerAgent) using C++ lib
    """
    def __init__(self, recv_buffer, svcname, portname):
        self.client = CM.MPI_Client(recv_buffer, svcname)
        pass

    def ping(self, uuid):
        self.send_string(uuid, len(uuid), 0, Tags.MPI_PING)

    def initial(self):
        self.client.initialize()

    def run(self):
        self.client.run()

    def send_int(self, int_data, msgsize, dest, tags):
        self.client.send_int(int_data, msgsize, dest, tags)

    def send_string(self, str ,msgsize, dest, tags):
        self.send_string(str, msgsize, dest, tags)

class MSG:
    def __init__(self, tag, pack):
        self.tag = tag
        self.pack = pack
