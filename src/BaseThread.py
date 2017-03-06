import threading
from logger import getLogger

logger = getLogger('BaseThread')

class BaseThread(threading.Thread):
    """
    Master Worker Base thread class
    """
    def __init__(self, name=None):
        if name is None:
            name = ""
        name = '.'.join(['DistJET.BaseThread',name])
        print("[BaseThread]: create new thread : %s"%name)
        super(BaseThread, self).__init__(name=name)
        self.setDaemon(1)                               # when this father thread terminate, child thread terminate as well
        self.__should_stop_flag = False
        #add log output: BaseThread object create:self.__class__.__name__
        logger.debug('BaseThread object created:%s', self.__class__.__name__)



    def get_stop_flag(self):
        return self.__should_stop_flag

    def stop(self):
        if not self.__should_stop_flag:
            # add log: Stopping thread:
            logger.debug('Stopping: %s', self.__class__.__name__)
            self.__should_stop_flag = True

