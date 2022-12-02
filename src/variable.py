
class Variable:
    def __init__(self, value, id, time) -> None:
        self.value = value
        self.id = id
        self.last_commit_time = time