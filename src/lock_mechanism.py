
class LockMechanism:

    def __init__(self) -> None:
        pass

    # Methods might include has a lock ,get a lock, release lock, release all locks, promote lock, check if lock exists

    def has_lock(self, transaction_number, variable, sites):
        present_all_sites = False
        if variable%2 ==0:
            present_all_sites = True
        return False

    def get_read_lock(self, transaction_number, variable):
        return False
    
    def get_write_lock(self, transaction_number, variable):
        return False

    def promote_lock(self,transaction_number, variable):
        self.get_write_lock(transaction_number,variable)
        return

    def release_lock(self,transaction_number, variable, sites):
        return True

    def release_all_locks(self, transaction_number, sites):
        locks = []
        for site in sites:
            locks = [lock for lock in site.lock_table if lock.transaction==transaction_number]
            for lock in locks:
                site.lock_table.remove(lock)
        return
    
    def is_read_locked(self, variable, sites):
        present_all_sites = False
        if variable%2 ==0:
            present_all_sites = True
        if present_all_sites:
            lock = []
            for site in sites:
                lock = [lock for lock in site.lock_table if lock.variable==variable and lock.type==1]
                if(len(lock)>0):
                    return True
        else:
            site_number = 1 + variable%10
            lock = [lock for lock in sites[site_number-1].lock_table if lock.variable==variable and lock.type==1]
            if(len(lock)>0):
                return True
        return False
    
    def is_write_locked(self, variable, sites):
        present_all_sites = False
        if variable%2 ==0:
            present_all_sites = True
        if present_all_sites:
            lock = []
            for site in sites:
                lock = [lock for lock in site.lock_table if lock.variable==variable and lock.type==2]
                if(len(lock)>0):
                    return True
        else:
            site_number = 1 + variable%10
            lock = [lock for lock in sites[site_number-1].lock_table if lock.variable==variable and lock.type==2]
            if(len(lock)>0):
                return True
        return False