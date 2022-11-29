
class TransactionManager:
    processed_data = []
    wait_list = []
    time = 0
    def __init__(self) -> None:
        self.time = 0
        print("Init")

    def initialize_replicas():
        return

    def initialize_variables():
        return

    def hello(self):
        print("Hello")

    def beginTransaction(self, transactionNumber, time, opType):
        t = Operation(opType, time, transactionNumber)
        operationHistory.append(t)
        print(t.time)
        
    def detectDeadlocks():
        return False

    def opProcess(self,line):
        time = time + 1
        allOperations = line.split("\n")
        for eachOperation in allOperations:
            if eachOperation == "begin(":
                #initialise new transaction function
                print("Insert begin() function")
            elif eachOperation == "fail(":
                #insert site fail function
                print("Site fail")
            elif eachOperation == "R(":
                #Read operation. eg. R(T1,x1). Execute write() function
                print("Transaction reads x_n")
            elif eachOperation == "W(":
                #Write operation. eg. W(T2,x8,88) . Execute write() function
                print("Transaction writes value to variable x_n")
            elif eachOperation == "end(":
                #Transaction t ends. Execute end() function
                print("Transaction writes value to variable x_n")
            elif eachOperation == "recover(":
                #eg. recover(2) => recover site 2.
                print("Recover site function to be executed")