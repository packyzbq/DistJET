import os
import subprocess
import Task

class IApplication:
    def __init__(self):
        self.app_boot=[]
        self.res_dir = ""
        self.data = []  #保存数据的路径
        self.args = {}
        self.task_list = {}  # tid:task
        self.task_reslist = {} # tid: result of task

        self._task_index = 0

        self.app_init_boot=[]
        self.app_init_data=[]

    def set_init_boot(self, init_boot):
        self.app_init_boot.append(init_boot)

    def set_init_data(self, init_data):
        self.app_init_data.append(init_data)

    def set_boot(self, boot_list):
        self.app_boot.append(boot_list)

    def set_resdir(self, res_dir):
        self.res_dir = res_dir

    def set_data(self, data):
        self.data = data

    def set_args(self, **kwargs):
        self.args = kwargs

    def get_task_by_id(self,tid):
        try:
            return self.task_list[tid]
        except KeyError:
            return None

    def create_tasks(self):
        raise NotImplementedError

    def task_done(self, tid):
        raise NotImplementedError

    def task_update(self,tid,task_status):
        """
        update the status of task for task monitor
        :param tid:
        :param task_status:
        :return:
        """
        pass

class UnitTestApp(IApplication):

    def __init__(self):
        IApplication.__init__(self)


    def split_data(self):
        if not os.environ.has_key('JUNOTESTROOT'):
            #TODO logging set env
            pass
        execdir = os.environ['JUNOTESTROOT']
        child = subprocess.Popen([execdir+'/python/JunoTest/junotest', 'UnitTest','list'], stdout=subprocess.PIPE)
        out = child.communicate()
        case = out[0].split('\n')[1:-1]
        return case

    def create_tasks(self):
        if self.args.has_key('data') and self.args['data'] == 'all':
            cases = self.split_data()
        else:
            cases = self.data
        for case in cases:
            task = Task.Task(self._task_index)
            task.initial(self.app_boot[0], data=case, res_dir=self.res_dir)
            self.task_list[self._task_index] = task
            self._task_index += 1

        with open(self.res_dir+'/summary.log','w+') as resfile:
            resfile.write('-------------------- result of TestCase --------------------\n')

    def task_done(self, tid):
        #TODO update tasks info
        #analyze result log file according to task data
        if self.analyze_log(self.task_list[tid].data):
            self.task_reslist[tid] = True
        else:
            self.task_reslist[tid] = False
        with open(self.res_dir+'/summary.log','w+') as resfile:
            if self.task_reslist[tid]:
                resfile.write(self.task_list[tid].data + '  SUCCESS\n')
            else:
                resfile.write(self.task_list[tid].data + '  ERROR\n')


    def analyze_log(self, logname):
        with open(self.res_dir+'/'+logname) as logfile:
            for line in logfile:
                if line.find('ERROR') != -1:
                    return False
                else:
                    return True