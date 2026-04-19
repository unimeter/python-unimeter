class UnimeterError(Exception):
    pass

class ConnectionError(UnimeterError):
    pass

class BackpressureError(UnimeterError):
    """Server disk full or overloaded. Retry later."""
    pass

class RedirectError(UnimeterError):
    def __init__(self, new_addr: str):
        self.new_addr = new_addr
        super().__init__(f"redirect to {new_addr}")

class ServerError(UnimeterError):
    pass

class AlreadyExistsError(UnimeterError):
    pass

class NotFoundError(UnimeterError):
    pass
