## Author: Rushab Shah, Deepali Chugh
## File: data_manager.py
## Date: 12/03/2022
## Purpose: This class is used for simulating a Site and its associated functions
## Key functionalities: Initialization, maintaining a lock table, fail, recovery, site status check, adding/removing variables
##                      Maintaining a commit history for multi-version read

class DataManager:
    site_number = None
    lock_table = []
    var_store = []
    commit_history = []
    is_down = True

    def __init__(self,site_num) -> None:
        self.site_number = site_num
        self.is_down = True
        self.lock_table = []
        self.commit_history = []
        
    def erase_locktable(self):
        self.lock_table = []
        return

    def fail(self):
        self.is_down = True
        self.erase_locktable()
        return

    def recover(self):
        self.is_down = False
        return

    def isSiteDown(self):
        return self.is_down
    
    def add_var(self,variable):
        self.var_store.append(variable)
        return
    
    def set_var_array(self,var_array):
        self.var_store = var_array
        return

    