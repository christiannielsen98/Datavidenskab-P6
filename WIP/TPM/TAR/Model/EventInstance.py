class EventInstance:
    def __init__(self, id, start=None, end=None):
        self.id = id
        self.start = start
        self.end = end

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, EventInstance):
            return self.id == other.id
        return NotImplemented

    def __str__(self):
        return '{},{},{}'.format(self.id, self.start, self.end)
