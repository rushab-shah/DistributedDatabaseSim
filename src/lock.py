## Author: Rushab Shah, Deepali Chugh
## File: lock.py
## Date: 12/03/2022
## Purpose: This class is used for modelling a Lock. The lock has properties such as lock type, variable name
##           that is locked, the site at which the lock exists and the transaction that holds the lock


class Lock:
    def __init__(self, lockType, variable, site, transaction) -> None:
        self.lockType = lockType
        self.variable = variable
        self.site = site
        self.transaction = transaction

    