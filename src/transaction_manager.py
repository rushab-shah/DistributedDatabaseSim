
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
    
    time = 0

    ######################################################################
    ## Initialization methods: __init__ , initialize_sites, initialize_site_variables
    ######################################################################
    def __init__(self) -> None:
        print("Initializing Transaction Manager")
        self.time = 0
        self.initialize_sites()
        print("Transaction Manager Initialized")

    def initialize_sites(self):
        print("Initializing Sites")
        for i in range(0,10):
            # Because sites are numbered 1 onwards while array is 0 indexed
            site = DataManager(i+1)
            self.sites.append(site)
            self.initialize_site_variables(i+1)
        print("Sites Initialized")
        return

    def initialize_site_variables(self,site_number):
        # print("Initializing Variables for Site "+str(site_number))
        var_store = []
        for id in range(1,21):
            if id % 2 == 0:
                var_obj = Variable(id*10,id)
                var_store.append(var_obj)
            elif site_number == (1 + id % 10):
                var_obj = Variable(id*10,id)
                var_store.append(var_obj)
        self.sites[site_number-1].set_var_array(var_store)
        return

    ######################################################################
    ## transaction start methods: beginTransaction, beginROTransaction
    ######################################################################
    def beginTransaction(self, transactionNumber, opType):
        o = Operation(opType, self.time, transactionNumber)
        t = Transaction(transactionNumber, beginTime=self.time, isReadOnly=False)
        self.operationHistory.append(o)
        self.activeTransactions[transactionNumber] = t
        # print(t.isReadOnly)
    
    def beginROTransaction(self, transactionNumber, opType):
        o = Operation(opType, self.time, transactionNumber)
        t = Transaction(transactionNumber, beginTime=self.time, isReadOnly=True)
        self.operationHistory.append(o)
        self.activeTransactions[transactionNumber] = t
        # print(t.isReadOnly)

    ######################################################################
    ## transaction start methods: Read Operation Methods
    ######################################################################
    def readValue(self, opType, transactionNum, variable_Name):
        currLock = LockMechanism()
        # Check if transactionNum has write or read lock using currLock.has_lock(). 
        # Response will be a lock object or None
        # If None check if any other transaction has a "Write" lock using currLock.is_write_locked()
        # Response again will be a lock obj or None
        # In this case if response is None, go ahead and get a read lock using currLock.get_read_lock()
        # Else use the lock obj to determine who has the lock then block transaction and add depende.
        currTxnHasLock = currLock.has_lock(transactionNum, variable_Name, self.sites)
        o = Operation(opType, self.time, transactionNum)
        t = self.activeTransactions.get(transactionNum)
        # self.operationHistory.append(o)

        if currTxnHasLock == None:
            lockedByOtherTransactions = currLock.is_write_locked(variable_Name, self.sites)
            if lockedByOtherTransactions != None:
                #Set lock for transaction txn to R
                self.blockedTransactions[transactionNum] = t
                self.blockedOperations.append(o)
                self.add_dependency(transactionNum, lockedByOtherTransactions.transaction)
            else:
                read_lock = currLock.get_lock(0,transactionNum, variable_Name, self.sites)
                if read_lock != None:
                    print("")
                    # READ
                else:
                    # Block or Abort?
                    print("Blocked")
                    self.blockedTransactions[transactionNum] = t
                    self.blockedOperations.append(o)
                    #block
        else:
            print("")
            # have a lock so read
            # READ
        return


    def readOp(self, opType, transactionNum, variableName):
        if transactionNum in self.activeTransactions.keys():
            # print(self.activeTransactions.values())
            self.readValue(opType, transactionNum, variableName)


    ######################################################################
    ## transaction start methods: Write Operation Methods
    ######################################################################
    def writeValue(self, opType, transaction_num, variable_name, x_value):
        o = Operation(opType, self.time, transaction_num)
        t = self.activeTransactions.get(transaction_num)

        currLock = LockMechanism()
        currTxnHasLock = currLock.has_lock(transaction_num, variable_name, self.sites) 

        writeLockedByOtherTransactions = currLock.is_write_locked(variable_name, self.sites)
        readLockedByOtherTransactions = currLock.is_read_locked(variable_name, self.sites)

        if currTxnHasLock == None:
            if (readLockedByOtherTransactions != None) or (writeLockedByOtherTransactions != None):
                self.blockedTransactions[transaction_num] = t
                self.blockedOperations.append(o)
                if readLockedByOtherTransactions != None:
                    for locks in readLockedByOtherTransactions:
                        self.add_dependency(transaction_num, locks.transaction)
                if writeLockedByOtherTransactions != None:
                    self.add_dependency(transaction_num, writeLockedByOtherTransactions.transaction)
            else:
                newLock = currLock.get_lock(1, transaction_num, variable_name, x_value)
                ## If getting the lock is not successful, None will be returned so change the if condition accordingly
                if newLock != None:
                    self.operationHistory.append(o)
                    t.add_to_commit(o)
                else:
                    self.blockedTransactions[transaction_num] = t
                    self.blockedOperations.append(o)
                ## else: block transaction & operation
                
        elif currTxnHasLock.lockType == 0:
            if readLockedByOtherTransactions != None:
                for locks in readLockedByOtherTransactions:
                    self.add_dependency(transaction_num, locks.transaction)
                    ## Also block transac and operation
            elif readLockedByOtherTransactions == None:
                currLock.promote_lock(transaction_num, variable_name, self.sites, )
                self.operationHistory.append(o)
                t.add_to_commit(o)
                
        else:
            newLock = currLock.get_lock(1, transaction_num, variable_name, self.sites)
            ## If getting the lock is not successful, None will be returned so change the if condition accordingly
            if newLock != None:
                self.operationHistory.append(o)
                t.add_to_commit(o)
            else:
                self.blockedTransactions[transaction_num] = t
                self.blockedOperations.append(o)
        return
    
    def writeOp(self, opType, transaction_num, variable_name, x_value ):
        #Check for locks, check if site is available, else write on other sites
        if transaction_num in self.activeTransactions.keys():
            self.writeValue(opType, transaction_num, variable_name, x_value)
        return False

    ######################################################################
    ## deadlock detection: add_dependency, remove_dependency,  detect deadlocks
    ######################################################################
    def add_dependency(self,requesting_transaction, holding_transaction):
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


    def detect_deadlocks(self):
        cycles = nx.simple_cycles(self.dependency_graph)
        if(len(list(cycles))):
            return True
        return False

    def handle_deadlock(self):
        # Abort the youngest transaction
        min_age = 99999999
        transaction_to_abort = -1
        for tnum in self.activeTransactions.keys():
            if self.get_transaction_age(tnum) < min_age:
                min_age = self.get_transaction_age(tnum)
                transaction_to_abort = tnum
        self.abort(transaction_to_abort)
        return

    ######################################################################
    ## transaction end methods: end transaction, abort, commit, can commit
    ######################################################################
    def end_transaction(self,transaction_number):
        if(self.can_commit(transaction_number)):
            t = self.activeTransactions.pop(transaction_number)
            self.commit(t)
            self.expiredTransactions[transaction_number] = t
        else:
            self.abort(transaction_number)
            self.expiredTransactions[transaction_number] = self.activeTransactions.pop(transaction_number)
            print("Transaction "+str(transaction_number)+" aborted")

        ## Release all locks for transaction_number
        locker = LockMechanism()
        locker.release_all_locks(transaction_number,self.sites)
        return
    
    def abort(self, transaction_number):
        #TODO STUFF
        # self.activeTransactions[transaction_number]
        return

    def commit(self, transaction):
        for write_op in transaction.to_commit:
            variable = write_op.variable
            variable_val  = write_op.variableValue
            if variable % 2 ==0:
                for site in self.sites:
                    target_var = [v for v in site.var_store if v.id == variable]
                    if(len(target_var)==1):
                        index = site.var_store.index(target_var[0])
                        site.var_store[index].value = variable_val
            else:
                site_num = 1 + variable%10
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
        transaction_start_time = self.activeTransactions[transaction_number].beginTime
        age = self.time - transaction_start_time
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

        ##### Begin() Transaction ######
        if eachOperation.startswith("begin("):
            transactionNum = (eachOperation.split("("))[1][:-2]
            opType = eachOperation[:5]
            print("Starting Transaction "+str(transactionNum))
            self.beginTransaction(transactionNum, opType)
            
        elif eachOperation.startswith("beginRO("):
            #beginRO(T3) means T3 txn begins and is read only
            transactionNum = (eachOperation.split("("))[1][:-2]
            opType = eachOperation[:7]
            print("Starting Read-Only Transaction "+str(transactionNum))
            self.beginROTransaction(transactionNum, opType)
            # print("Insert beginRO() function")

        elif eachOperation.startswith("fail("):
            #insert site fail function
            site = self.extract_id_from_operation(eachOperation)
            self.fail(eachOperation[int(site)])
            print("Site" +str(site) +" fail")

        elif eachOperation.startswith("R("):
            #Read operation. eg. R(T1,x1). Execute write() function
            split_readOp = eachOperation.split(",")
            opType = eachOperation[0]
            txn = split_readOp[0][2:]
            var_x = split_readOp[1][1:-1]
            self.readOp(opType, txn, var_x)
            print("Transaction reads x_n")

        elif eachOperation.startswith("W("):
            #Write operation. eg. W(T2,x8,88) . Execute write() function
            split_writeOp = eachOperation.split(",")
            opType = eachOperation[0]
            txn = split_writeOp[0][2:]
            var_x = split_writeOp[1][1:]
            write_val = split_writeOp[2][:-1]
            self.writeOp(opType, txn, var_x, write_val)
            print("Transaction writes value to variable x_n")
        
        elif eachOperation.startswith("end("):
            #Transaction t ends. Execute end() function
            ### Get transaction id
            self.end_transaction((eachOperation.split("("))[1][:-2])
            print("Transaction " +str((eachOperation.split("("))[1][:-2])+" ended")
        
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


