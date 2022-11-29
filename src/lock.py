
# A lock class to emulate a lock

class Lock:
    def __init__(self, type, variable, site, transaction) -> None:
        self.type = type
        self.variable = variable
        self.site = site
        self.transaction = transaction

    