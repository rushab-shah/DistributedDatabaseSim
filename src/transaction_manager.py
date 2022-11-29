
from operation import Operation
from transaction import Transaction

operationHistory = []

class TransactionManager:
    isReadOnly = False
    processed_data = []
    wait_list = []
    time = 0
    def __init__(self) -> None:
        self.time = 0

    def hello(self):
        print("Hello")

    def beginTransaction(self, transactionNumber, time, opType):
        o = Operation(opType, time, transactionNumber)
        t = Transaction(transactionNumber, time, isReadOnly=False)
        operationHistory.append(o)
        # print(t.isReadOnly)
    
    def beginROTransaction(self, transactionNumber, time, opType):
        o = Operation(opType, time, transactionNumber)
        t = Transaction(transactionNumber, time, isReadOnly=True)
        operationHistory.append(o)
        # print(t.isReadOnly)

    def detectDeadlocks():
        return False

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
            print("Recover site function to be executed")

eachOperation = "begin(T1)"
tm = TransactionManager()
tm.opProcess(eachOperation)