class ServiceException(Exception):
    """
    Base class for Service exceptions
    """

    pass


class LifecycleError(ServiceException):
    """
    Raised when an action would violate the service lifecycle rules.
    """

    pass


class DaemonTaskExit(ServiceException):
    """
    Raised when an action would violate the service lifecycle rules.
    """

    pass


class TooManyChildrenException(ServiceException):
    """
    Raised when a service adds too many children. It is a sign of task leakage
    that needs to be prevented.
    """

    pass
