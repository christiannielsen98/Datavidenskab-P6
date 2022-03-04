from .Relation import *
from .Utils import *
from .EventInstance import *
from .Event import*
from .Pattern import*


class Node:
    def __init__(self, ids_event, bitmap, max_supp, patterns=None):
        self.ids_event = ids_event
        self.bitmap = bitmap
        self.max_supp = max_supp
        self.patterns = patterns

    def get_ids_events(self):
        return tuple(self.ids_event)

    # def get_list_events_label(self):
    #     return tuple([event.label for event in self.events])

    def get_bitmap(self):
        return self.bitmap

    def get_support(self):
        return len(self.bitmap)

    def get_patterns(self):
        return self.patterns

    def to_dict(self, event_table, num_sequence, event_instance_table=None):
        result = {}
        events = [event_table[id] for id in self.ids_event]
        if len(events) == 1:  # level 1
            event_name = ','.join(self.get_ids_events())
            time_intervals = {}
            if event_instance_table:
                for sid in self.bitmap:
                    list_intances = events[0].get_list_instance_at_sequence_id(sid)
                    list_time = []
                    for instance in list_intances:
                        obj = event_instance_table[instance]
                        time = obj.start, obj.end
                        list_time.append(tuple(time))
                    time_intervals[sid] = list_time
            result['name_node'] = event_name
            if time_intervals:
                result['supp'] = len(self.bitmap) / num_sequence
                result['conf'] = len(self.bitmap) / self.max_supp
                result['time'] = time_intervals
        else:
            event_labels = self.get_ids_events()
            pattern_result = []
            for pattern in self.patterns:
                obj = pattern.to_dict(event_labels, self.max_supp, num_sequence,  event_instance_table)
                pattern_result.append(obj)
            event_name = ','.join(event_labels)

            if pattern_result:
                result['name_node'] = event_name
                result['patterns'] = pattern_result

        return result

    def __str__(self):
        return ','.join(self.ids_event)
