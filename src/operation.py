class Operation:
  # This will contain a blueprint of an operation. Which is each line in the input
  def __init__(self, opType, time, transactionNumber, variable = None, variableValue = None) -> None:
    self.transactionNumber = transactionNumber
    self.opType = opType
    self.variable = variable
    self.variableValue = variableValue
    self.time = time
      