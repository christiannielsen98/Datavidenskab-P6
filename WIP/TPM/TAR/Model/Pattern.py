from typing import Sequence
from .Relation import *
import copy


class Pattern:
    def __init__(self, list_relation, seq_eventInsIds):
        '''
        list_relation: list [Relation.Follows, Relation.Overlaps]
        seq_eventInsIds: {
            sequence ID: [(id_instance_a1,id_instance_b1), (id_instance_a2,id_instance_b2),...]
            }
        '''
        self.list_relation = tuple(list_relation)
        self.seq_eventInsIds = copy.deepcopy(seq_eventInsIds)
        self.bitmap = tuple(self.seq_eventInsIds.keys())

    def get_bitmap(self):
        return self.seq_eventInsIds.keys()

    def get_instance_at_sequence_id(self, key):
        return self.seq_eventInsIds[key]

    def get_list_relation(self):
        return list(self.list_relation)

    def __getitem__(self, key):
        return self.seq_eventInsIds[key]

    def to_dict(self, event_labels, maxSup, num_sequence, event_instance_table=None):
        '''
            relation_symbol = "102010000000000"
            "1| 0|2 0|10 0|000 0|0000"
            AB: 1 => A1B
            ABC: 210 => A2B*B1C*A0C
            ABCD: 012120 => A0
            ABCDE: 01200120102

            A1B B0C A2C C0D A1D B0D ....
        '''
        pattern_name = []
        size = 1
        relation_symbol = self.list_relation
        while(relation_symbol):
            slit_relation = relation_symbol[:size]
            relation_symbol = relation_symbol[size:]
            last_event = event_labels[size - 1]
            append_event = event_labels[size]
            relation = last_event + str(slit_relation[0]) + append_event
            pattern_name.append(relation)
            for i in range(0, size-1):
                current_event = event_labels[i]
                relation = current_event + str(slit_relation[i+1]) + append_event
                pattern_name.append(relation)
            size += 1
        pattern_name = "*".join(pattern_name)
        result_dict = {}
        result_dict['pattern'] = pattern_name
        result_dict['supp'] = len(self.bitmap) / num_sequence
        result_dict['conf'] = len(self.bitmap) / maxSup

        time_interval = {}
        for sid in self.seq_eventInsIds:
            list_intances = self.get_instance_at_sequence_id(sid)
            list_time = []
            for instances in list_intances:
                times = []
                for instance in instances:
                    if event_instance_table:
                        obj = event_instance_table[instance]
                        time = obj.start, obj.end
                        times.append(time)
                    else:
                        times.append(instance)
                list_time.append(tuple(times))
            time_interval[sid] = list_time
        result_dict['time'] = time_interval
        return result_dict

    def __len__(self):
        return len(self.seq_eventInsIds)

    def __str__(self):
        tmp = [str(relation.name) for relation in self.list_relation]
        return ', '.join(tmp)
