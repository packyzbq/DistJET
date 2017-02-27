import logging

#FORMAT = '%(asctime)s: %(message)s'

#logging.basicConfig(level=logging.DEBUG, format=FORMAT)

def getLogger(name, level=logging.INFO):
    format = logging.Formatter('%(asctime)s] %(threadName)s %(levelname)s: %(message)s')
    handler = logging.FileHandler('DistJET.'+name+'.log')
    handler.setFormatter(format)

    logger = logging.getLogger('DistJET.'+name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

def setlevel(level, logger):
    logger.setLevel(level)