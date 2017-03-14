import src.Application

def run():
    app = src.Application.UnitTestApp()
    app.set_boot("./run.sh")
    app.set_data("all")
    app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET/test")

    return app