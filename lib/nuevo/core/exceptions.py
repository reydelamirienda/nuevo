
class NuevoException(Exception):
    pass

class NotFoundException(NuevoException):
    pass

class ExistsException(NuevoException):
    pass