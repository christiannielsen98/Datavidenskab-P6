class Event:
    def __init__(self, id, seq_eventInsIds=None):
        '''
        seq_eventInsIds is dictionary:
            keys: sequence ID
            values: list id of EventInstance
        '''
        self.id = id
        self.seq_eventInsIds = seq_eventInsIds
        # self.bitmap = None
        # if event_instances != None:
        #     self.bitmap = tuple(self.event_instances.keys())

    # def get_sequence_ids(self):
    #     return self.event_instances.keys()

    def get_list_instance_at_sequence_id(self, key):
        return self.seq_eventInsIds[key]

    def get_bitmap(self):
        return self.seq_eventInsIds.keys()

    def get_support(self):
        return len(self.seq_eventInsIds)

    def is_satisfied_support(self, minsup):
        supp = len(self.seq_eventInsIds)
        return supp >= minsup

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, Event):
            return self.id == other.id
        return NotImplemented

    def __str__(self):
        return self.id
