## Author: Rushab Shah, Deepali Chugh
## File: commit.py
## Date: 12/03/2022
## Purpose: This class is used for modelling a commit of a write operation

class Commit:
    def __init__(self,transaction:int,variable_name:int,variable_value:int,time:int) -> None:
        self.transaction = transaction
        self.variable_name = variable_name
        self.variable_value = variable_value
        self.time = time