## Author: Rushab Shah, Deepali Chugh
## File: transaction.py
## Date: 12/03/2022
## Purpose: This class is used for modelling a transaction. A transaction has properties such has the associated number
##          the start and end time, site access records as well as whether or not its ReadOnly

class Transaction:
  # This will contain a blueprint of an operation. Which is each line in the input
  def __init__(self, transactionNumber, isReadOnly, beginTime, endTime = None) -> None:
    self.transactionNumber = transactionNumber
    self.beginTime = beginTime
    self.endTime = endTime
    self.isReadOnly = isReadOnly
    self.to_commit = []
    self.site_first_access_record = {}

  def add_to_commit(self, operation):
    self.to_commit.append(operation)
    return

  def clear_commit(self):
    self.to_commit = []
    return

  def first_accessed_site(self,site_num,time):
    if self.site_first_access_record.get(site_num)== None:
      self.site_first_access_record[site_num] = time  
    return