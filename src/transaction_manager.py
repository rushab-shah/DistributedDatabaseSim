
from operation import Operation
from transaction import Transaction
from data_manager import DataManager
from variable import Variable

class TransactionManager:
    isReadOnly = False
    processed_data = []
    wait_list = []
    sites = []
    operationHistory = []
    activeTransactions = {}
    blockedTransactions = {}
    expiredTransactions = {}
    dependency_graph = []
    dependency_graph_nodes = {}
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
        print("Initializing Variables for Site "+str(site_number))
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
    def beginTransaction(self, transactionNumber, time, opType):
        o = Operation(opType, time, transactionNumber)
        t = Transaction(transactionNumber, time, isReadOnly=False)
        self.operationHistory.append(o)
        self.activeTransactions[transactionNumber] = t
        # print(t.isReadOnly)
    
    def beginROTransaction(self, transactionNumber, time, opType):
        o = Operation(opType, time, transactionNumber)
        t = Transaction(transactionNumber, time, isReadOnly=True)
        self.operationHistory.append(o)
        self.activeTransactions[transactionNumber] = t
        # print(t.isReadOnly)


    ######################################################################
    ## deadlock detection: add_dependency, remove_dependency,  detect deadlocks, visit node
    ######################################################################
    def add_dependency(self,requesting_transaction, holding_transaction):
        dependency = (requesting_transaction, holding_transaction)
        self.dependency_graph.append(dependency)
        self.dependency_graph_nodes.add(requesting_transaction)
        self.dependency_graph_nodes.add(holding_transaction)
        return

    def remove_dependency(self,requesting_transaction, holding_transaction):
        result = [dependency for dependency in self.dependency_graph if dependency[0]==requesting_transaction
                    and dependency[1]==holding_transaction]
        if(len(result)==1):
            self.dependency_graph.remove(result[0])
            if(requesting_transaction in self.dependency_graph_nodes):
                self.dependency_graph_nodes.remove(requesting_transaction)
            if(holding_transaction in self.dependency_graph_nodes):
                self.dependency_graph_nodes.remove(holding_transaction)
        return


    def detect_deadlocks(self):
        visited = [False for i in range(1,len(self.dependency_graph_nodes)+1)]
        for edge in self.dependency_graph:
            cycle_detected = self.visit(edge,visited)
            if(cycle_detected):
                return True
            else:
                visited = [False for i in range(1,len(self.dependency_graph_nodes)+1)]
        return False
    
    def visit(self, edge, visited):
        if(visited[edge[0]]==True):
            return True
        else:
            visited[edge[0]] = True
            return self.visit(edge[1])

    ######################################################################
    ## Transaction helper methods: get_transaction_age
    ######################################################################
    def get_transaction_age(self,transaction_number):
        # fetch the transaction from list of active transactions
        transaction_start_time = self.activeTransactions[transaction_number].beginTime
        age = self.time - transaction_start_time
        return age


    ######################################################################
    ## Site specific functions: fail, recover
    ######################################################################
    def fail(self, index):
        if index < len(self.sites):
            self.sites[index-1].fail()
        return

    def recover(self, index):
        if index < len(self.sites):
            self.sites[index-1].recover()
        return


    ######################################################################
    ## Method to process op strings from file/cmd: opProcess
    ######################################################################
    def opProcess(self,line):
        self.time = self.time + 1

        ##### Begin() Transaction ######
        if eachOperation.startswith("begin("):
            transactionNum = eachOperation[-3:-1]
            opType = eachOperation[:5]
            print("Starting Transaction "+str(transactionNum))
            self.beginTransaction(transactionNum, self.time, opType)
            
        elif eachOperation.startswith("beginRO("):
            #beginRO(T3) means T3 txn begins and is read only
            transactionNum = eachOperation[-3:-1]
            opType = eachOperation[:7]
            print("Starting Read-Only Transaction "+str(transactionNum))
            self.beginROTransaction(transactionNum, self.time, opType)
            # print("Insert beginRO() function")

        elif eachOperation.startswith("fail("):
            #insert site fail function
            temp = len(eachOperation)
            site = None
            if(temp==7):
                site = eachOperation[5]
            elif(temp==8):
                site = eachOperation[5] +''+eachOperation[6]
            else:
                return
            self.fail(eachOperation[int(site)])
            print("Site" +str(site) +" fail")

        elif eachOperation.startswith("R("):
            #Read operation. eg. R(T1,x1). Execute write() function
            print("Transaction reads x_n")

        elif eachOperation.startswith("W("):
            #Write operation. eg. W(T2,x8,88) . Execute write() function
            print("Transaction writes value to variable x_n")
        
        elif eachOperation.startswith("end("):
            #Transaction t ends. Execute end() function
            print("Transaction writes value to variable x_n")
        
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


######################################################################
## Testing code
######################################################################
eachOperation = "begin(T1)"
tm = TransactionManager()
tm.opProcess(eachOperation)
