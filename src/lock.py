
# A lock class to emulate a lock

class Lock:
    def __init__(self, lockType, variable, site, transaction) -> None:
        self.lockType = lockType
        self.variable = variable
        self.site = site
        self.transaction = transaction

    