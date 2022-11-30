
class LockMechanism:

    def __init__(self) -> None:
        pass

    # Methods might include has a lock ,get a lock, release lock, release all locks, promote lock, check if lock exists

    def has_lock(self, transaction_number, variable, sites):

        return False

    def get_read_lock(self, transaction_number, variable):
        return False
    
    def get_write_lock(self, transaction_number, variable):
        return False

    def promote_lock(self,transaction_number, variable):
        self.get_write_lock(transaction_number,variable)
        return

    def release_lock(self,transaction_number, variable):
        return True

    def release_all_locks(self, transaction_number):
        return
    
    def is_read_locked(self, variable, sites):
        return
    
    def is_write_locked(self, variable, sites):
        return