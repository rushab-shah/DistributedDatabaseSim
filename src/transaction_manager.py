
from operation import Operation
from transaction import Transaction

class TransactionManager:
    isReadOnly = False
    processed_data = []
    wait_list = []
    sites = []
    operationHistory = []
    activeTransactions = []
    blockedTransactions = []
    expiredTransactions = []
    time = 0

    def __init__(self) -> None:
        self.time = 0
        self.initialize_sites()

    def initialize_sites(self):
        for i in range(0,10):
            # Because sites are numbered 1 onwards while array is 0 indexed
            site = DataManager(i+1)
            self.sites.append(site)
        return

    def beginTransaction(self, transactionNumber, time, opType):
        o = Operation(opType, time, transactionNumber)
        t = Transaction(transactionNumber, time, isReadOnly=False)
        self.operationHistory.append(o)
        self.activeTransactions.append(t)
        # print(t.isReadOnly)
    
    def beginROTransaction(self, transactionNumber, time, opType):
        o = Operation(opType, time, transactionNumber)
        t = Transaction(transactionNumber, time, isReadOnly=True)
        self.operationHistory.append(o)
        self.activeTransactions.append(t)
        # print(t.isReadOnly)

    def detectDeadlocks():
        return False
    
    def fail(self, index):
        if index < len(self.sites):
            self.sites[index-1].fail()
        return

    def recover(self, index):
        if index < len(self.sites):
            self.sites[index-1].recover()
        return

    def fail(self, index):
        if index < len(self.sites):
            self.sites[index-1].fail()
        return

    def recover(self, index):
        if index < len(self.sites):
            self.sites[index-1].recover()
        return

    def opProcess(self,line):
        self.time = self.time + 1

        ##### Begin() Transaction ######
        if eachOperation.startswith("begin("):
            transactionNum = eachOperation[-3:-1]
            opType = eachOperation[:5]
            self.beginTransaction(transactionNum, self.time, opType)
            
        elif eachOperation.startswith("beginRO("):
            #beginRO(T3) means T3 txn begins and is read only
            transactionNum = eachOperation[-3:-1]
            opType = eachOperation[:7]
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
            print("Site fail")

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
            print("Recover site function to be executed")

eachOperation = "begin(T1)"
tm = TransactionManager()
tm.opProcess(eachOperation)