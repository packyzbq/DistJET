
class Policy:
    """
        Collection of policy values. Empty by default.
        """
    ## False => ignore failed tasks and continue running; True => redo the failed tasks
    REDO_IF_FAILED_TASKS = False
    # False=> ignore failed application initialize and then the worker quit; True => reinitialize
    REDO_IF_FAILED_APPINI = False
    # the limit times to reassign tasks or initial worker
    REDO_LIMITS = 3

    # Worker ping interval, default = 10s
    PING_DELAY = 10