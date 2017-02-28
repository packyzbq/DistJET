import src
from src import RunMaster

app = src.Application.UnitTestApp()
app.set_boot("/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Release/J16v2r1/offline/Validation/JunoTest/python/JunoTest/junotest")
app.set_data("JunoTest")
app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET/test")

applications = []
applications.append(app)

master = src.RunMaster.Master(applications)
print('master start running')
master.startProcessing()