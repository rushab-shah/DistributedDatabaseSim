
from operation import Operation
from transaction import Transaction
from data_manager import DataManager
from variable import Variable
from lock import Lock
from lock_mechanism import LockMechanism
import networkx as nx

class TransactionManager:
    isReadOnly = False
    processed_data = []
    wait_list = []
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
    
    time = 0

    def operations_left(self):
        if len(self.blockedOperations) > 0:
            return True
        else:
            return False

    def finish_remaining_operations(self):
        if len(self.blockedOperations) > 0:
            for transaction_num in self.blockedTransactions:
                self.activeTransactions[transaction_num] = self.blockedTransactions[transaction_num]
            self.blockedTransactions = {}

            pending_ops = self.blockedOperations
            self.blockedOperations = []

            for op in pending_ops:
                if self.debug:
                    print("OPTYPE "+str(op.opType))
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
                var_obj = Variable(id*10,id)
                var_store.append(var_obj)
            elif site_number == (1 + id % 10):
                var_obj = Variable(id*10,id)
                var_store.append(var_obj)
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
                lockedByOtherTransactions = self.lock_system.is_write_locked(variable_Name, self.sites)
                if lockedByOtherTransactions != None:
                    #Set lock for transaction txn to R
                    if self.debug:
                        print("Transaction blocked because another transaction has a write lock")
                    self.blockedTransactions[transactionNum] = t
                    self.blockedOperations.append(o)
                    self.add_dependency(transactionNum, lockedByOtherTransactions.transaction)
                    self.activeTransactions.pop(transactionNum)
                else:
                    read_lock = self.lock_system.get_lock(0,transactionNum, variable_Name, self.sites)
                    if read_lock != None:
                        if self.debug:
                            print("Got read lock at "+str(read_lock.site))
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
                if self.debug:
                    print("Transaction T"+str(transactionNum)+" already has a lock on variable x"+str(variable_Name))
                return
        else:
            if self.debug:
                print("Read Only--------------")
            return


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
                newLock = self.lock_system.get_lock(1, transaction_num, variable_name, self.sites)
                ## If getting the lock is not successful, None will be returned so change the if condition accordingly
                if newLock != None:
                    self.operationHistory.append(o)
                    t.add_to_commit(o)
                    if self.debug:
                        print("Transaction "+transaction_num+" writes value "+str(x_value)+" to variable x"+str(variable_name))
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
                self.lock_system.promote_lock(transaction_num, variable_name, self.sites, currTxnHasLock.site)
                self.operationHistory.append(o)
                t.add_to_commit(o)
                if self.debug:
                    print("Transaction "+transaction_num+" writes value "+str(x_value)+" to variable x"+str(variable_name))
                
        else:
            # newLock = self.lock_system.get_lock(1, transaction_num, variable_name, self.sites)
            # ## If getting the lock is not successful, None will be returned so change the if condition accordingly
            # if newLock != None:
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
        if self.debug:
            print("Clearing dependencies for transaction "+str(expiring_transaction))
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
    ######################################################################
    def end_transaction(self,transaction_number):
        if self.blockedTransactions.get(transaction_number) == None:
            if(self.can_commit(transaction_number)):
                if self.debug:
                    print("Transaction "+str(transaction_number)+" can commit")
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
                self.expiredTransactions[transaction_number] = self.activeTransactions.pop(transaction_number)
                print("Transaction "+str(transaction_number)+" aborted")
        else:
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
                    target_var = [v for v in site.var_store if v.id == variable]
                    if(len(target_var)==1):
                        index = site.var_store.index(target_var[0])
                        site.var_store[index].value = variable_val
            else:
                site_num = 1 + int(variable)%10
                target_var = [v for v in self.sites[site_num-1].var_store if v.id == variable]
                if(len(target_var)==1):
                    index = self.sites[site_num-1].var_store.index(target_var[0])
                    self.sites[site_num-1].var_store[index].value = variable_val
        print("Transaction "+str(transaction.transactionNumber)+" commited")
        return
    
    def can_commit(self, transaction_number):
        ##TODO Need to check for odd even variable ids?
        for site in self.sites:
            if site.isSiteDown() == True:
                return False
        return True

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
    def fail(self, index):
        if index < len(self.sites):
            self.sites[index-1].fail()
            # Post failure steps
        return

    def recover(self, index):
        if index < len(self.sites):
            self.sites[index-1].recover()
            # Post recovery steps
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
            transactionNum = (eachOperation.split("("))[1][1:-2]
            opType = eachOperation[:5]
            if self.debug:
                print("Starting Transaction "+str(transactionNum))
            self.beginTransaction(transactionNum, opType)
            
        elif eachOperation.startswith("beginRO("):
            #beginRO(T3) means T3 txn begins and is read only
            transactionNum = (eachOperation.split("("))[1][1:-2]
            opType = eachOperation[:7]
            if self.debug:
                print("Starting Read-Only Transaction "+str(transactionNum))
            self.beginROTransaction(transactionNum, opType)
            # print("Insert beginRO() function")

        elif eachOperation.startswith("fail("):
            #insert site fail function fail(2)
            site = self.extract_id_from_operation(eachOperation)
            self.fail(eachOperation[int(site)])
            if self.debug:
                print("Site" +str(site) +" fail")

        elif eachOperation.startswith("R("):
            #Read operation. eg. R(T1,x1). Execute read() function
            split_readOp = eachOperation.split("(")
            opType = split_readOp[0]
            txn_and_var = split_readOp[1].split(",")
            txn = txn_and_var[0][1:]
            var_x = txn_and_var[1].split(")")[0][1:]
            self.readOp(opType, txn, var_x)

        elif eachOperation.startswith("W("):
            #Write operation. eg. W(T2,x8,88) . Execute write() function
            split_writeOp = eachOperation.split("(")
            opType = split_writeOp[0]
            txn_var_val = split_writeOp[1].split(",")
            txn = txn_var_val[0][1:]
            var_x = txn_var_val[1][1:]
            value_x = txn_var_val[2].split(")")[0]
            self.writeOp(opType, txn, var_x, value_x)
        
        elif eachOperation.startswith("end("):
            #Transaction t ends. Execute end() function
            ### Get transaction id
            temp = eachOperation.split("(")[1]
            # if self.debug:
            #     print(eachOperation)
            #     temp = eachOperation.split("(")[1]
            #     print(temp.split(")")[0][1])
            self.end_transaction(temp.split(")")[0][1])
        
        elif eachOperation.startswith("recover("):
            #eg. recover(2) => recover site 2.
            temp = len(eachOperation)
            site = None
            if(temp==7):
                site = eachOperation[5]
            elif(temp==8):
                site = eachOperation[5] +''+eachOperation[6]
            else:
                return
            self.recover(eachOperation[int(site)])
            if self.debug:
                print("Site "+str(site)+" Recovered")

        elif eachOperation == "dump()":
            self.dump()

        return


######################################################################
## Testing code
######################################################################
# eachOperation = "R(T1)"
# tm = TransactionManager()s
# tm.opProcess(eachOperation)

# tm.add_dependency(0,1)
# tm.add_dependency(0,2)
# tm.add_dependency(1,2)
# tm.add_dependency(2,0)
# print(tm.detect_deadlocks())


