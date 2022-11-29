
class DataManager:
    site_number = None
    lock_table = []
    var_store = []
    is_down = True

    def __init__(self,site_num) -> None:
        self.site_number = site_num
        self.is_down = True
        
    def erase_locktable(self):
        self.lock_table = []
        return

    def isDown(self):
        return self.is_down

    