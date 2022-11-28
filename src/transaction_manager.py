
class TransactionManager:
    processed_data = []
    time = 0
    def __init__(self) -> None:
        print("Init")


    def hello(self):
        print("Hello")

    def opProcess(self,line):
        time = time + 1
        if eachOperation.startswith() == "begin(":
            #initialise new transaction function
            print("Insert begin() function")
        elif eachOperation.startswith() == "beginRO(":
            #beginRO(T3) means T3 txn begins and is read only
            print("Insert beginRO() function")
        elif eachOperation.startswith() == "fail(":
            #insert site fail function
            print("Site fail")
        elif eachOperation.startswith() == "R(":
            #Read operation. eg. R(T1,x1). Execute write() function
            print("Transaction reads x_n")
        elif eachOperation.startswith() == "W(":
            #Write operation. eg. W(T2,x8,88) . Execute write() function
            print("Transaction writes value to variable x_n")
        elif eachOperation.startswith() == "end(":
            #Transaction t ends. Execute end() function
            print("Transaction writes value to variable x_n")
        elif eachOperation.startswith() == "recover(":
            #eg. recover(2) => recover site 2.
            print("Recover site function to be executed")