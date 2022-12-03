## Author: Rushab Shah, Deepali Chugh
## File: variable.py
## Date: 12/03/2022
## Purpose: This class is used for modelling a Variable. A variable has properties such as its name and value,
##          & whether its available for read or not (after site failure)

class Variable:
    def __init__(self, value, id, time) -> None:
        self.value = value
        self.id = id
        self.last_commit_time = time
        self.available_for_read = True