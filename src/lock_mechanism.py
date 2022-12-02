from lock import Lock

class LockMechanism:

    def __init__(self) -> None:
        pass

    # Methods might include has a lock ,get a lock, release lock, release all locks, promote lock, check if lock exists

    def has_lock(self, transaction_number, variable, sites):
        present_all_sites = False
        if int(variable) % 2 ==0:
            present_all_sites = True
        if present_all_sites:
            for site in sites:
                for lock in site.lock_table:
                    if lock.variable == variable and lock.transaction == transaction_number:
                        return lock
        else:
            site_num = 1 + int(variable)%10
            for lock in sites[site_num-1].lock_table:
                if lock.variable == variable and lock.transaction == transaction_number:
                    return lock
        return None

    def get_lock(self, lock_type, transaction_number, variable, sites):
        # lock_type = 0 : read; lock_type = 1 : write
        # First check which site is up. In order
        all_sites = False
        if int(variable) % 2 ==0:
            all_sites = True
        if all_sites:
            index = -1
            for i in range(0,len(sites)):
                if sites[i].isSiteDown() != True:
                    index = i
                    break
            if index == -1:
                return None
            lock = Lock(lock_type,variable,index+1,transaction_number)
            print("Obtaining "+str(lock_type)+" lock for "+str(transaction_number)+" for variable x"+str(variable) +" at site "+str(index+1))
            sites[i].lock_table.append(lock)
            return lock
        else:
            site_num = (1+ int(variable)%10)
            if(sites[site_num-1].isSiteDown() != True):
                lock = Lock(lock_type,variable,site_num,transaction_number)
                sites[site_num-1].lock_table.append(lock)
                print("Obtaining "+str(lock_type)+" lock for "+str(transaction_number)+" for variable x"+str(variable) +" at site "+str(site_num))
                return lock
            else:
                return None
    

    def promote_lock(self,transaction_number, variable,sites):
        # self.get_lock(1,transaction_number,variable,sites)
        ## Just change lock type to write
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
        if int(variable)%2 ==0:
            present_all_sites = True
        if present_all_sites:
            locks = []
            for site in sites:
                temp = [lock for lock in site.lock_table if lock.variable==variable and lock.lockType==0]
                if(len(temp)>0):
                    locks.append(temp)
            if len(locks) > 0:
                return locks
        else:
            site_number = 1 + int(variable)%10
            locks = [lock for lock in sites[site_number-1].lock_table if lock.variable==variable and lock.lockType==0]
            if(len(locks)>0):
                return locks
        return None
    
    def is_write_locked(self, variable, sites):
        present_all_sites = False
        if int(variable)%2 ==0:
            present_all_sites = True
        if present_all_sites:
            lock = []
            for site in sites:
                lock = [lock for lock in site.lock_table if lock.variable==variable and lock.lockType==1]
                if(len(lock)>0):
                    return lock[0]
        else:
            site_number = 1 + int(variable)%10
            lock = [lock for lock in sites[site_number-1].lock_table if lock.variable==variable and lock.lockType==1]
            if(len(lock)>0):
                return lock[0]
        return None