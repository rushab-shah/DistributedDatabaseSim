## Author: Rushab Shah, Deepali Chugh
## File: lock_mechanism.py
## Date: 12/03/2022
## Purpose: This class is used for simulating a locking system.
## Key features: Ability to check if there are existing read/write locks on a variable. Checking if a given transaction
##               has a lock already. There's also functionality to release locks for a transaction.

from lock import Lock

class LockMechanism:

    def __init__(self) -> None:
        pass

    lock_dict = {0:"read",1:"write"}
    debug = False

    # Methods might include has a lock ,get a lock, release lock, release all locks, promote lock, check if lock exists

    def has_lock(self, transaction_number, variable, sites):
        present_all_sites = False
        if int(variable) % 2 ==0:
            present_all_sites = True
        if present_all_sites:
            for site in sites:
                for lock in site.lock_table:
                    if int(lock.variable) == int(variable) and int(lock.transaction) == int(transaction_number):
                        return lock
        else:
            site_num = 1 + int(variable)%10
            for lock in sites[site_num-1].lock_table:
                if int(lock.variable) == int(variable) and int(lock.transaction) == int(transaction_number):
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
                    var_ind = [ind for ind, x in enumerate(sites[i].var_store) if int(x.id) == int(variable)]
                    if len(var_ind) == 0:
                        index = i
                        break
                    elif len(var_ind)==1 and sites[i].var_store[var_ind[0]].available_for_read:
                        index = i
                        break

            if index == -1:
                return None
            lock = Lock(lock_type,variable,index+1,transaction_number)
            if self.debug:
                print("Obtaining "+str(self.lock_dict[lock_type])+" lock for T"+str(transaction_number)+" for variable x"+str(variable) +" at site "+str(index+1))
            sites[index].lock_table.append(lock)
            return lock
        else:
            site_num = (1+ int(variable)%10)
            if(sites[site_num-1].isSiteDown() != True): 
                lock = Lock(lock_type,variable,site_num,transaction_number)
                sites[site_num-1].lock_table.append(lock)
                if self.debug:
                    print("Obtaining "+str(self.lock_dict[lock_type])+" lock for T"+str(transaction_number)+" for variable x"+str(variable) +" at site "+str(site_num))
                return lock
            else:
                return None

    def get_write_lock(self, transaction_number, variable, sites):
        all_sites = False
        if int(variable) % 2 ==0:
            all_sites = True
        if all_sites:
            temp = []
            for i in range(0,len(sites)):
                if sites[i].isSiteDown() != True:
                    # if self.debug:
                    #     print("Obtaining "+str(self.lock_dict[1])+" lock for T"+str(transaction_number)+" for variable x"+str(variable) +" at site "+str(i+1))
                    sites[i].lock_table.append(Lock(1,variable,i+1,transaction_number))
                    temp.append(Lock(1,variable,i+1,transaction_number))
            return temp
        else:
            site_num = (1+ int(variable)%10)
            if(sites[site_num-1].isSiteDown() != True): 
                lock = Lock(1,variable,site_num,transaction_number)
                sites[site_num-1].lock_table.append(lock)
                # if self.debug:
                #     print("Obtaining write lock for T"+str(transaction_number)+" for variable x"+str(variable) +" at site "+str(site_num))
                return lock
            else:
                return None   

    def promote_lock(self,transaction_number, variable,sites, site_num):

        index = [ind for ind, lock in enumerate(sites[site_num-1].lock_table) if int(lock.transaction)==int(transaction_number) and int(lock.variable) == int(variable)]
        if(len(index)==1):
            sites[site_num-1].lock_table.pop(index[0])
        
        success = self.get_write_lock(transaction_number,variable,sites)
        if success!=None:
            if self.debug:
                print("Promoted lock for T"+str(sites[site_num-1].lock_table[index[0]].transaction)+" at site "+str(site_num))
        else:
            if self.debug:
                print("Promoted lock failed")
        return

    def release_lock(self,transaction_number, variable, sites):
        return True

    def release_all_locks(self, transaction_number, sites):
        locks = []
        for site in sites:
            locks = [lock for lock in site.lock_table if int(lock.transaction)==int(transaction_number)]
            for lock in locks:
                site.lock_table.remove(lock)
        return
    
    def is_read_locked(self, variable, sites, requesting_transaction):
        present_all_sites = False
        if int(variable)%2 ==0:
            present_all_sites = True
        if present_all_sites:
            locks = []
            for site in sites:
                temp = [lock for lock in site.lock_table if int(lock.variable)==int(variable) and int(lock.transaction)!=int(requesting_transaction)  and int(lock.lockType)==0]
                if(len(temp)>0):
                    locks += temp
            if len(locks) > 0:
                return locks
            else:
                return None
        else:
            site_number = 1 + int(variable)%10
            locks = [lock for lock in sites[site_number-1].lock_table if int(lock.variable)==int(variable) and int(lock.transaction)!=int(requesting_transaction) and int(lock.lockType)==0]
            if(len(locks)>0):
                return locks
            else:
                return None
    
    def is_write_locked(self, variable, sites, requesting_transaction):
        present_all_sites = False
        if int(variable)%2 ==0:
            present_all_sites = True
        if present_all_sites:
            lock = []
            for site in sites:
                lock = [lock for lock in site.lock_table if int(lock.variable)==int(variable) and int(lock.transaction)!=int(requesting_transaction) and int(lock.lockType)==1]
                if(len(lock)>0):
                    return lock[0]
        else:
            site_number = 1 + int(variable)%10
            lock = [lock for lock in sites[site_number-1].lock_table if int(lock.variable)==int(variable) and int(lock.transaction)!=int(requesting_transaction) and int(lock.lockType)==1]
            if(len(lock)>0):
                return lock[0]
        return None
