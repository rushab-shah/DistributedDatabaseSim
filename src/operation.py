## Author: Rushab Shah, Deepali Chugh
## File: operation.py
## Date: 12/03/2022
## Purpose: This class is used for modelling an operation of a transaction. An operation has properties such as
##          the transaction with which it is associated(not in case of fail/recover),
##          the operation type (read/write/begin/end/fail/recover), the variable associated and the timestamp

class Operation:
  # This will contain a blueprint of an operation. Which is each line in the input
  def __init__(self, opType, time, transactionNumber, variable = None, variableValue = None) -> None:
    self.transactionNumber = transactionNumber
    self.opType = opType
    self.variable = variable
    self.variableValue = variableValue
    self.time = time
      