## Author: Rushab Shah, Deepali Chugh
## File: transaction_manager.py
## Date: 12/03/2022
## Purpose: The transaction manager (TM) plays a crucial role in the simulation of our system. The transaction manager
##          is responsible for processing each operation that comes as an input from the input file or command line. 
##          The transaction manager is responsible for initializing and managing sites. It is responsible for communicating
##          with the locking mechanism to obtain, release & check for locks. It is responsible for actually carrying out
##          the operations in a transaction. It also checks for deadlocks, aborts, blocks and commits transactions.


from operation import Operation
from transaction import Transaction
from data_manager import DataManager
from variable import Variable
from lock import Lock
from lock_mechanism import LockMechanism
import networkx as nx
from incident import Incident
from commit import Commit

class TransactionManager:
    sites = []
    operationHistory = []
    blockedOperations = []
    activeTransactions = {}
    blockedTransactions = {}
    expiredTransactions = {}
    dependency_graph_edges = []
    dependency_graph = nx.DiGraph()
    check_deadlock = False
    debug = False
    site_incidents = []
    time = 0

    def operations_left(self):
        if len(self.blockedOperations) > 0:
            return True
        else:
            return False

    def finish_remaining_operations(self):
        if len(self.blockedOperations) > 0:
            for transaction_num in self.blockedTransactions:
                if self.debug:
                    print("Resuming transaction T"+str(transaction_num))
                self.activeTransactions[transaction_num] = self.blockedTransactions[transaction_num]
            self.blockedTransactions = {}

            pending_ops = self.blockedOperations
            self.blockedOperations = []

            for op in pending_ops:
                # if self.debug:
                #     print("OPTYPE "+str(op.opType))
                if op.opType=="R":
                    self.readValue(op.opType,op.transactionNumber,op.variable)
                elif op.opType=="W":
                    self.writeValue(op.opType,op.transactionNumber,op.variable,op.variableValue)
                elif op.opType=="end":
                    self.end_transaction(op.transactionNumber)

            pending_ops = []
        else:
            return

    ######################################################################
    ## Initialization methods: __init__ , initialize_sites, initialize_site_variables
    ######################################################################
    def __init__(self) -> None:
        self.time = 0
        self.initialize_sites()
        self.lock_system = LockMechanism()

    def toggle_debugger(self,debug):
        self.debug = debug
        self.lock_system.debug = debug
        return

    def initialize_sites(self):
        if self.debug:
            print("Initializing Sites")
        for i in range(0,10):
            # Because sites are numbered 1 onwards while array is 0 indexed
            self.sites.append(DataManager(i+1))
            self.initialize_site_variables(i+1)
        if self.debug:    
            print("Sites Initialized")
        return

    def initialize_site_variables(self,site_number):
        var_store = []
        for id in range(1,21):
            if id % 2 == 0:
                var_obj = Variable(id*10,id,self.time)
                var_store.append(var_obj)
            elif site_number == (1 + id % 10):
                var_obj = Variable(id*10,id,self.time)
                var_store.append(var_obj)
            # if self.debug:
            # print("Adding commit for x"+str(id)+" at site "+str(site_number)+" at "+str(self.time))
            self.sites[site_number-1].commit_history.append(Commit(-1,id,id*10,self.time))
        self.sites[site_number-1].set_var_array(var_store)
        self.sites[site_number-1].is_down = False
        return

    ######################################################################
    ## transaction start methods: beginTransaction, beginROTransaction
    ######################################################################
    def beginTransaction(self, transactionNumber, opType):
        o = Operation(opType, self.time, transactionNumber)
        t = Transaction(transactionNumber, False, beginTime=self.time)
        self.operationHistory.append(o)
        self.activeTransactions[transactionNumber] = t
        # print(t.isReadOnly)
    
    def beginROTransaction(self, transactionNumber, opType):
        o = Operation(opType, self.time, transactionNumber)
        t = Transaction(transactionNumber, True, beginTime=self.time)
        self.operationHistory.append(o)
        self.activeTransactions[transactionNumber] = t
        # print(t.isReadOnly)

    ######################################################################
    ## transaction start methods: Read Operation Methods
    ######################################################################
    def readValue(self, opType, transactionNum, variable_Name):
        o = Operation(opType, self.time, transactionNum, variable_Name)
        t = self.activeTransactions.get(transactionNum)

        if t.isReadOnly == False:
            currTxnHasLock = self.lock_system.has_lock(transactionNum, variable_Name, self.sites)
            if currTxnHasLock == None:
                lockedByOtherTransactions = self.lock_system.is_write_locked(variable_Name, self.sites, transactionNum)
                if lockedByOtherTransactions != None:
                    #Set lock for transaction txn to R
                    if self.debug:
                        print("Transaction blocked because another transaction has a write lock")
                    self.blockedTransactions[transactionNum] = t
                    self.blockedOperations.append(o)
                    self.add_dependency(transactionNum, lockedByOtherTransactions.transaction)
                    self.activeTransactions.pop(transactionNum)
                else:
                    dep = self.check_if_lock_req_in_queue(transactionNum,variable_Name)
                    if dep != None:
                        self.add_dependency(dep[0],dep[1])
                        if self.debug:
                            print("Transaction blocked as a prior transaction needs the lock first")
                        self.blockedTransactions[transactionNum] = t
                        self.blockedOperations.append(o)
                        self.activeTransactions.pop(transactionNum)
                        return
                    else:
                        read_lock = self.lock_system.get_lock(0,transactionNum, variable_Name, self.sites)
                        if read_lock != None:
                            if self.debug:
                                print("Got read lock at "+str(read_lock.site))

                            index = [ind for ind, x in enumerate(self.sites[read_lock.site-1].var_store) if int(x.id) == int(variable_Name)]
                            if(len(index)==1):
                                print("x"+str(variable_Name)+":"+str(self.sites[int(read_lock.site)-1].var_store[index[0]].value))
                            
                            self.activeTransactions[transactionNum].first_accessed_site(read_lock.site, self.time)
                            return
                        else:
                            # Block or Abort?
                            if self.debug:
                                print("Transaction blocked as site is unavailable")
                            self.blockedTransactions[transactionNum] = t
                            self.blockedOperations.append(o)
                            self.activeTransactions.pop(transactionNum)
                            return
            else:
                # currTxnHasLock
                index = [ind for ind, x in enumerate(self.sites[currTxnHasLock.site-1].var_store) if x.id == variable_Name]
                if(len(index)==1):
                    print("x"+str(variable_Name)+":"+str(self.sites[currTxnHasLock.site-1].var_store[index].value))
                if self.debug:
                    print("Transaction T"+str(transactionNum)+" already has a lock on variable x"+str(variable_Name))
                return
        else:
            if self.debug:
                print("Transaction T"+str(transactionNum)+" is read only")
            start_time = t.beginTime
            read_success = self.read_correct_version(variable_Name,start_time, transactionNum)
            # if read_success and self.debug:
            #         print("RO successful")
            # else:
            #         print("RO op Failed")
            return

    def read_correct_version(self, variable, start_time, transactionNum):
        site_num = self.get_site_for_ro(variable,start_time)
        if site_num != None:
            commits = [x for x in self.sites[site_num-1].commit_history if int(x.time) < int(start_time) and int(x.variable_name)==int(variable)]
            if len(commits) > 0:
                sorted_commits = sorted(commits, key=lambda x: x.time)
                read_val = sorted_commits[len(sorted_commits)-1].variable_value
                print("x"+str(variable)+": "+str(read_val))
                return True
            else:

                if self.debug:
                    print("commit list empty")
        else:
            if self.debug:
                print("Aborting read only transaction T"+str(transactionNum))
            self.abort(transactionNum)
        return False

    def get_site_for_ro(self, variable, start_time):
        if int(variable)%2==0:
            commit_time = -1

            # First checking latest commit time
            for site in self.sites:
                commits = [commit for commit in site.commit_history if commit.time < start_time 
                and commit.variable_name == variable]
                # if self.debug:
                #     print("Checking for sites for RO before start time "+str(start_time))
                if len(commits)>0:
                    sorted_commits = sorted(commits, key=lambda x: x.time)
                    if commit_time < sorted_commits[len(sorted_commits)-1].time:
                        commit_time = sorted_commits[len(sorted_commits)-1].time

            # Now checking for site failures between commit_time and start_time
            candidate_site = -1
            for site in self.sites:
                failures = [x for x in self.site_incidents if int(x.site_number) == int(site.site_number) 
                            and int(x.time) < int(start_time) and int(x.time) > int(commit_time)]
                if len(failures) == 0:
                    return site.site_number
            if candidate_site == -1:
                return None
        else:
            site_num = 1 + int(variable)%10
            if self.sites[site_num-1].isSiteDown():
                return None
            else:
                return site_num
        return None

    def readOp(self, opType, transactionNum, variableName):
        if transactionNum in self.activeTransactions.keys():
            # print(self.activeTransactions.values())
            self.readValue(opType, transactionNum, variableName)


    ######################################################################
    ## transaction start methods: Write Operation Methods
    ######################################################################
    def writeValue(self, opType, transaction_num, variable_name, x_value):
        o = Operation(opType, self.time, transaction_num, variable_name, x_value)
        t = self.activeTransactions.get(transaction_num)
        currTxnHasLock = self.lock_system.has_lock(transaction_num, variable_name, self.sites) 

        writeLockedByOtherTransactions = self.lock_system.is_write_locked(variable_name, self.sites, transaction_num)
        readLockedByOtherTransactions = self.lock_system.is_read_locked(variable_name, self.sites, transaction_num)


        if currTxnHasLock == None:
            if (readLockedByOtherTransactions != None) or (writeLockedByOtherTransactions != None):
                self.blockedTransactions[transaction_num] = t
                self.blockedOperations.append(o)
                self.activeTransactions.pop(transaction_num)
                print("Transaction "+str(transaction_num)+" is blocked because lock is held by someone else")
                if readLockedByOtherTransactions != None:
                    for locks in readLockedByOtherTransactions:
                        if(transaction_num!=locks.transaction):
                            self.add_dependency(transaction_num, locks.transaction)
                if writeLockedByOtherTransactions != None:
                    self.add_dependency(transaction_num, writeLockedByOtherTransactions.transaction)
            else:
                dep = self.check_if_lock_req_in_queue(transaction_num,variable_name)
                if dep!=None: 
                    self.add_dependency(dep[0],dep[1])
                    if self.debug:
                        print("Transaction blocked as a prior transaction needs the lock first")
                    self.blockedTransactions[transaction_num] = t
                    self.blockedOperations.append(o)
                    self.activeTransactions.pop(transaction_num)
                    return
                else:
                    newLock = self.lock_system.get_write_lock(transaction_num, variable_name, self.sites)
                    ## If getting the lock is not successful, None will be returned so change the if condition accordingly
                    if newLock != None:
                        self.operationHistory.append(o)
                        t.add_to_commit(o)
                        print("T"+str(transaction_num)+" writes x"+str(variable_name))

                        if int(variable_name)%2==0:
                            for lock in newLock:                        
                                self.activeTransactions[transaction_num].first_accessed_site(lock.site, self.time)
                        else:
                            self.activeTransactions[transaction_num].first_accessed_site(newLock.site, self.time)
                        # if self.debug:
                        #     print("Transaction "+transaction_num+" writes value "+str(x_value)+" to variable x"+str(variable_name))
                    else:
                        self.blockedTransactions[transaction_num] = t
                        self.blockedOperations.append(o)
                        self.activeTransactions.pop(transaction_num)
                        print("Transaction "+str(transaction_num)+" is blocked because no sites available")
                    ## else: block transaction & operation
                
        elif currTxnHasLock.lockType == 0:
            if readLockedByOtherTransactions != None:
                self.blockedTransactions[transaction_num] = t
                self.blockedOperations.append(o)
                self.activeTransactions.pop(transaction_num)
                print("Transaction "+str(transaction_num)+" is blocked")
                for locks in readLockedByOtherTransactions:
                    if(transaction_num!=locks.transaction):
                        self.add_dependency(transaction_num, locks.transaction)  
            elif readLockedByOtherTransactions == None:
                dep = self.check_if_lock_req_in_queue(transaction_num,variable_name)
                if dep!=None:
                    self.add_dependency(dep[0],dep[1])
                    if self.debug:
                        print("Transaction blocked as a prior transaction needs the lock first")
                    self.blockedTransactions[transaction_num] = t
                    self.blockedOperations.append(o)
                    self.activeTransactions.pop(transaction_num)
                    return
                else:
                    self.lock_system.promote_lock(transaction_num, variable_name, self.sites, currTxnHasLock.site)
                    self.operationHistory.append(o)
                    t.add_to_commit(o)
                    print("T"+str(transaction_num)+" writes x"+str(variable_name))
                    # if self.debug:
                    #     print("Transaction "+transaction_num+" writes value "+str(x_value)+" to variable x"+str(variable_name))
                
        else:
            if self.debug:
                print("Transaction T"+str(transaction_num)+" already has a write lock on x"+str(variable_name))
            self.operationHistory.append(o)
            t.add_to_commit(o)
            if self.debug:
                print("Transaction "+transaction_num+" writes value "+str(x_value)+" to variable x"+str(variable_name))
            # else:
            #     self.blockedTransactions[transaction_num] = t
            #     self.blockedOperations.append(o)
            #     self.activeTransactions.pop(transaction_num)
            #     if self.debug:
            #         print("Transaction "+str(transaction_num)+" is blocked because no sites available")
        return
    
    def writeOp(self, opType, transaction_num, variable_name, x_value ):
        #Check for locks, check if site is available, else write on other sites
        if transaction_num in self.activeTransactions.keys():
            self.writeValue(opType, transaction_num, variable_name, x_value)
        # Check if transaction is blocked
        return False

    ######################################################################
    ## deadlock detection: add_dependency, remove_dependency,  detect deadlocks
    ######################################################################
    def check_if_lock_req_in_queue(self,requesting_transaction, variable):
        i = len(self.blockedOperations)-1
        while i>=0:
            op = self.blockedOperations[i]
            if op.opType == "W" and int(op.variable)==int(variable):
                prior_transaction = op.transactionNumber
                return (requesting_transaction,prior_transaction)
            i -=1 
        return None


    def add_dependency(self,requesting_transaction, holding_transaction):
        self.check_deadlock = True
        dependency = (requesting_transaction, holding_transaction)
        self.dependency_graph_edges.append(dependency)
        self.dependency_graph.add_edge(requesting_transaction,holding_transaction)
        return

    def remove_dependency(self,requesting_transaction, holding_transaction):
        result = [dependency for dependency in self.dependency_graph_edges if dependency[0]==requesting_transaction
                    and dependency[1]==holding_transaction]
        if(len(result)==1):
            self.dependency_graph_edges.remove(result[0])
            self.dependency_graph.remove_edge(requesting_transaction,holding_transaction)
        return

    def clear_dependencies(self,expiring_transaction):
        # if self.debug:
        #     print("Clearing dependencies for transaction "+str(expiring_transaction))
        edges = [ e for e in self.dependency_graph_edges if e[1]==expiring_transaction]
        for edge in edges:
            self.dependency_graph_edges.remove(edge)
        return

    def detect_deadlocks(self):
        self.check_deadlock = False
        cycles = nx.simple_cycles(self.dependency_graph)
        if(len(list(cycles))):
            print("Deadlock detected")
            return True
        return False

    def handle_deadlock(self):
        # Abort the youngest transaction
        min_age = 99999999
        transaction_to_abort = -1
        transactions_to_check = self.activeTransactions | self.blockedTransactions
        for tnum in transactions_to_check.keys():
            if self.get_transaction_age(tnum) < min_age:
                min_age = self.get_transaction_age(tnum)
                transaction_to_abort = tnum
        if transaction_to_abort!=-1:
            print("Aborted Transaction "+str(transaction_to_abort)+" for handling deadlock")
            self.abort(transaction_to_abort)
        return

    ######################################################################
    ## transaction end methods: end transaction, abort, commit, can commit
    #####################################################################
    def end_transaction(self,transaction_number):
        if self.activeTransactions.get(transaction_number) != None:
            if(self.can_commit(transaction_number)):
                # if self.debug:
                #     print("Transaction "+str(transaction_number)+" can commit")
                t = self.activeTransactions.pop(transaction_number)
                t.endTime = self.time
                self.commit(t)
                ## Release all locks for transaction_number
                self.lock_system.release_all_locks(transaction_number,self.sites)

                # Release all dependencies
                self.clear_dependencies(transaction_number)
                self.expiredTransactions[transaction_number] = t
            else:
                self.abort(transaction_number)
                # self.expiredTransactions[transaction_number] = self.activeTransactions.pop(transaction_number)
                print("T"+str(transaction_number)+" aborts because can't commit🧐")
            # Resume blocked ops
            # if self.debug:
            #     print("Resuming any blocked ops")
            self.finish_remaining_operations()
        elif self.blockedTransactions.get(transaction_number) != None:
            # Can't end transaction since its blocked
            op = Operation("end",self.time,transaction_number)
            self.blockedOperations.append(op)
        return
    
    def abort(self, transaction_number):
        if self.activeTransactions.get(transaction_number)!= None:
            t = self.activeTransactions.pop(transaction_number)
            t.endTime = self.time
            ## Release all locks for transaction_number
            self.lock_system.release_all_locks(transaction_number,self.sites)

            # Release all dependencies
            self.clear_dependencies(transaction_number)
            self.delete_blocked_ops(transaction_number)
            self.expiredTransactions[transaction_number] = t
        elif self.blockedTransactions.get(transaction_number)!= None:
            t = self.blockedTransactions.pop(transaction_number)
            t.endTime = self.time
            ## Release all locks for transaction_number
            self.lock_system.release_all_locks(transaction_number,self.sites)

            # Release all dependencies
            self.clear_dependencies(transaction_number)
            self.delete_blocked_ops(transaction_number)
            self.expiredTransactions[transaction_number] = t
        return

    def delete_blocked_ops(self, transaction_number):
        ops = [ op for op in self.blockedOperations if op.transactionNumber == transaction_number]
        for op in ops:
            self.blockedOperations.remove(op)
        return

    def commit(self, transaction):
        for write_op in transaction.to_commit:
            variable = write_op.variable
            variable_val  = write_op.variableValue
            if int(variable) % 2 ==0:
                for site in self.sites:
                    if site.isSiteDown() != True:
                        site_num = site.site_number
                        if transaction.site_first_access_record.get(site_num)!=None and self.check_if_write_lock_at_site(site_num,transaction.transactionNumber,variable):
                            target_var = [v for v in site.var_store if int(v.id) == int(variable)]
                            if(len(target_var)==1):
                                # if self.debug:
                                #     print("T"+str(transaction.transactionNumber)+" commits "+str(variable)+" at "+str(site_num))
                                index = site.var_store.index(target_var[0])
                                site.var_store[index].value = variable_val
                                site.var_store[index].last_commit_time = self.time
                                site.commit_history.append(Commit(int(transaction.transactionNumber),int(variable),int(variable_val),self.time))
                                self.set_read_availability(site.site_number,variable)
            else:
                site_num = 1 + int(variable)%10
                target_var = [v for v in self.sites[site_num-1].var_store if int(v.id) == int(variable)]
                if(len(target_var)==1) and self.sites[site_num-1].isSiteDown() != True:
                    if transaction.site_first_access_record.get(site_num)!=None and self.check_if_write_lock_at_site(site_num,transaction.transactionNumber,variable):
                        index = self.sites[site_num-1].var_store.index(target_var[0])
                        self.sites[site_num-1].var_store[index].value = variable_val
                        self.sites[site_num-1].var_store[index].last_commit_time = self.time
                        self.sites[site_num-1].commit_history.append(Commit(int(transaction.transactionNumber),int(variable),int(variable_val),self.time))
        print("T"+str(transaction.transactionNumber)+" commits")
        return

    def check_if_write_lock_at_site(self,site_number, transaction_number, variable):
        lock = [x for x in self.sites[site_number-1].lock_table if int(x.variable)==int(variable) 
        and int(x.transaction)==int(transaction_number) and int(x.lockType)==1]
        if(len(lock)==1):
            return True
        return False
    
    def can_commit(self, transaction_number):
        # for site in self.sites:
        #     if site.isSiteDown() == True:
        #         return False
        
        if self.check_if_site_ever_failed(transaction_number):
            if self.debug:
                print("Site failed since first access")
            return False
        return True

    def check_if_site_ever_failed(self,transaction_num):
        t = self.activeTransactions[transaction_num]
        if len(t.site_first_access_record) > 0:
            for site in t.site_first_access_record:
                first_access = t.site_first_access_record.get(site)
                if  first_access!= None and self.failure_in_time_range(site,first_access):
                    return True
        return False

    def failure_in_time_range(self,site_number,first_access):
        incidents = [x for x in self.site_incidents if int(x.site_number) == int(site_number) and x.incident_type=="fail"
         and x.time_of_occurence > first_access]
        if len(incidents) > 0:
            return True
        return False    

    def dump(self):
        for site in self.sites:
            dump_str = "site "+str(site.site_number)+" - "
            for variable in site.var_store:
                dump_str += "x"+str(variable.id) +": "+str(variable.value)+", "
            print(dump_str)
        return

    ######################################################################
    ## Transaction helper methods: get_transaction_age, extract id from op
    ######################################################################
    def get_transaction_age(self,transaction_number):
        # fetch the transaction from list of active transactions
        if self.activeTransactions.get(transaction_number)!= None:
            transaction_start_time = self.activeTransactions[transaction_number].beginTime
            age = self.time - transaction_start_time
            return age
        elif self.blockedTransactions.get(transaction_number)!= None:
            transaction_start_time = self.blockedTransactions[transaction_number].beginTime
            age = self.time - transaction_start_time
            return age
        return age
    
    def extract_id_from_operation(self, operation):
        temp = len(operation)
        site = None
        if(temp==7):
            site = operation[5]
        elif(temp==8):
            site = operation[5] +''+operation[6]
        else:
            return site
        return site


    ######################################################################
    ## Site specific functions: fail, recover
    ######################################################################
    def fail(self, site_number):
        if site_number <= len(self.sites):
            # block transactions that had locks
            self.abort_transactions_at_site(site_number)

            self.sites[site_number-1].fail()
            incident_object = Incident(site_number,"fail",self.time)
            self.site_incidents.append(incident_object)
            # Post failure steps
        return

    def abort_transactions_at_site(self, site_num):
        transactions_to_abort = set()
        # if self.debug:
        #     print("ABORTING TRANSACTIONS")
        for lock in self.sites[site_num-1].lock_table:
            transactions_to_abort.add(lock.transaction)
        for transaction in transactions_to_abort:
            self.abort(transaction)
            print("T"+str(transaction)+" aborts")
        return

    def recover(self, site_number):
        if site_number < len(self.sites):
            self.sites[site_number-1].recover()
            incident_object = Incident(site_number,"recover",self.time)
            self.site_incidents.append(incident_object)
            self.reset_read_availability(site_number)
            # if self.debug:
            #     print("Resuming any blocked ops")
            self.finish_remaining_operations()
            # Post recovery steps
        return

    def reset_read_availability(self,site_number):
        for data in self.sites[site_number-1].var_store:
            if int(data.id)%2 == 0:
                data.available_for_read = False
        return

    def set_read_availability(self,site_number,variable):
        index = [ind for ind, x in enumerate(self.sites[site_number-1].var_store) if int(x.id)==int(variable)]
        if(len(index)==1):
            self.sites[site_number-1].var_store[index[0]].available_for_read = True
        return
    ######################################################################
    ## Method to process op strings from file/cmd: opProcess
    ######################################################################
    def opProcess(self,eachOperation):
        self.time = self.time + 1
        eachOperation = eachOperation.replace(" ", "")
        if self.check_deadlock:
            deadlock_present = self.detect_deadlocks()
            if deadlock_present:
                self.handle_deadlock()

        ##### Begin() Transaction ######
        if eachOperation.startswith("begin("):
            transactionNum_str = (eachOperation.split("("))[1]
            transactionNum = transactionNum_str.split(")")[0].strip()[1:]
            opType = eachOperation[:5]
            if self.debug:
                print("Starting Transaction "+str(transactionNum))
            self.beginTransaction(transactionNum, opType)
            
        elif eachOperation.startswith("beginRO("):
            #beginRO(T3) means T3 txn begins and is read only
            transactionNum_str = (eachOperation.split("("))[1]
            transactionNum = transactionNum_str.split(")")[0].strip()[1:]
            opType = eachOperation[:7]
            if self.debug:
                print("Starting Read-Only Transaction "+str(transactionNum))
            self.beginROTransaction(transactionNum, opType)
            # print("Insert beginRO() function")

        elif eachOperation.startswith("fail("):
            #insert site fail function fail(2)
            site_str = (eachOperation.split("("))[1]
            site_num = site_str.split(")")[0].strip()
            self.fail(int(site_num))

            if self.debug:
                print("Site " +str(site_num) +" fails")

        elif eachOperation.startswith("R("):
            #Read operation. eg. R(T1,x1). Execute read() function
            split_readOp = eachOperation.split("(")
            opType = split_readOp[0].strip()
            txn_and_var = split_readOp[1].split(",")
            txn = txn_and_var[0].strip()[1:]
            var_x = txn_and_var[1].split(")")[0].strip()[1:]
            self.readOp(opType, txn, var_x)

        elif eachOperation.startswith("W("):
            #Write operation. eg. W(T2,x8,88) . Execute write() function
            split_writeOp = eachOperation.split("(")
            opType = split_writeOp[0].strip()
            txn_var_val = split_writeOp[1].split(",")
            txn = txn_var_val[0].strip()[1:]
            var_x = txn_var_val[1].strip()[1:]
            value_x = txn_var_val[2].split(")")[0].strip()
            self.writeOp(opType, txn, var_x, value_x)
        
        elif eachOperation.startswith("end("):
            #Transaction t ends. Execute end() function
            ### Get transaction id
            temp = eachOperation.split("(")[1]
            # if self.debug:
            #     print(eachOperation)
            #     temp = eachOperation.split("(")[1]
            #     print(temp.split(")")[0][1])
            self.end_transaction(temp.split(")")[0].strip()[1])
        
        elif eachOperation.startswith("recover("):
            #eg. recover(2) => recover site 2.
            site_str = (eachOperation.split("("))[1]
            site_num = site_str.split(")")[0].strip()

            self.recover(int(site_num))
            if self.debug:
                print("Site "+str(site_num)+" Recovered")

        elif eachOperation.strip() == "dump()":
            self.dump()

        return
