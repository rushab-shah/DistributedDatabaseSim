class Operation:
  # This will contain a blueprint of an operation. Which is each line in the input
  def __init__(self, opType, time, transactionNumber, variable = None, variableValue = None, isReadOnly = None) -> None:
    self.transactionNumber = transactionNumber
    self.opType = opType
    self.variable = variable
    self.variableValue = variableValue
    self.time = time
    self.isReadOnly = isReadOnly

  def checkIsReadOnly(self):
    if self.opType == "beginRO":
      self.isReadOnly = True
    elif self.opType == "begin":
      self.isReadOnly = False
      