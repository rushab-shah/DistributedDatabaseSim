class Transaction:
  # This will contain a blueprint of an operation. Which is each line in the input
  def __init__(self, transactionNumber, beginTime, endTime = None, isReadOnly = None) -> None:
    self.transactionNumber = transactionNumber
    self.beginTime = beginTime
    self.endTime = endTime
    self.isReadOnly = isReadOnly
    self.to_commit = []

  def add_to_commit(self, operation):
    self.to_commit.append(operation)
    return

  def clear_commit(self):
    self.to_commit = []
    return