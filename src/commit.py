class Commit:
    def __init__(self,transaction:int,variable_name:int,variable_value:int,time:int) -> None:
        self.transaction = transaction
        self.variable_name = variable_name
        self.variable_value = variable_value
        self.time = time