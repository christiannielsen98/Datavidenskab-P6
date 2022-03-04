from posixpath import relpath
from .Model import *


class FTPMining:
    def __init__(self, database, minsup, minconf, epsilon, minOverlap, maxPatternSize):
        self.minsup = minsup
        self.minconf = minconf
        self.epsilon = epsilon
        self.minoverlap = minOverlap
        self.maxPatternSize = maxPatternSize
        self.EventInstanceTable = {}
        self.EventTable = {}
        self.Nodes = {}
        self.num_patterns = 0

        for idx, row in enumerate(database):
            start, end, id_event, sID = row
            start = int(start)
            end = int(end)
            eventIns = EventInstance(idx, start, end)
            self.EventInstanceTable[idx] = eventIns

            if id_event not in self.EventTable:
                event = Event(id_event, {sID: [idx]})
                self.EventTable[id_event] = event
            elif sID not in self.EventTable[id_event].seq_eventInsIds:
                self.EventTable[id_event].seq_eventInsIds[sID] = [idx]
            else:
                self.EventTable[id_event].seq_eventInsIds[sID].append(idx)

    def Find1Event(self):
        self.Nodes['level1'] = {}
        for id_event in self.EventTable:
            if self.EventTable[id_event].is_satisfied_support(self.minsup):
                bitmap = self.EventTable[id_event].get_bitmap()
                support = self.EventTable[id_event].get_support()
                node = Node(tuple([id_event]), bitmap, support)
                self.Nodes['level1'][(id_event)] = node

    def Find2FrequentPatterns(self):
        self.Nodes['level2'] = {}
        for node1_id in self.Nodes['level1']:
            node1 = self.Nodes['level1'][node1_id]
            node1_bitmap = set(node1.get_bitmap())
            node1_support = node1.get_support()
            for node2_id in self.Nodes['level1']:
                node2 = self.Nodes['level1'][node2_id]
                node2_bitmap = set(node2.get_bitmap())
                node2_support = node2.get_support()

                bitmap = node1_bitmap.intersection(node2_bitmap)

                if len(bitmap) >= self.minsup:
                    if (node1_support / node2_support < self.minconf) or (node2_support / node1_support < self.minconf):
                        continue
                    max_supp = max(node1_support, node2_support)
                    id_event1 = node1.ids_event[0]
                    id_event2 = node2.ids_event[0]
                    list_patterns = self.Find2Patterns(id_event1, id_event2, bitmap, max_supp)
                    if list_patterns:
                        node = Node((id_event1, id_event2), bitmap, max_supp, list_patterns)
                        self.Nodes['level2'][(id_event1, id_event2)] = node
                        self.num_patterns += len(list_patterns)

    def Find2Patterns(self, id_event1, id_event2, bitmap, max_supp):
        e1_f_e2_instances = {}  # follow
        e1_c_e2_instances = {}  # contain
        e1_o_e2_instances = {}  # overlap
        for sID in bitmap:
            list_event_instances1 = self.EventTable[id_event1].get_list_instance_at_sequence_id(sID)
            list_event_instances2 = self.EventTable[id_event2].get_list_instance_at_sequence_id(sID)
            for id_event_instances1 in list_event_instances1:
                time_1 = self.EventInstanceTable[id_event_instances1]
                for id_event_instances2 in list_event_instances2:
                    time_2 = self.EventInstanceTable[id_event_instances2]
                    relation = Utils.check_relation(time_1, time_2, id_event1, id_event2, self.epsilon, self.minoverlap)

                    if relation is Relation.Follows:
                        if sID in e1_f_e2_instances:
                            e1_f_e2_instances[sID].append((id_event_instances1, id_event_instances2))
                        else:
                            e1_f_e2_instances[sID] = [(id_event_instances1, id_event_instances2)]

                    elif relation is Relation.Contains:
                        if sID in e1_c_e2_instances:
                            e1_c_e2_instances[sID].append((id_event_instances1, id_event_instances2))
                        else:
                            e1_c_e2_instances[sID] = [(id_event_instances1, id_event_instances2)]

                    elif relation is Relation.Overlaps:
                        if sID in e1_o_e2_instances:
                            e1_o_e2_instances[sID].append((id_event_instances1, id_event_instances2))
                        else:
                            e1_o_e2_instances[sID] = [(id_event_instances1, id_event_instances2)]

        list_patterns = []

        support_follow = len(e1_f_e2_instances.keys())
        if support_follow >= self.minsup and support_follow/max_supp >= self.minconf:
            follow_pattern = Pattern([Relation.Follows], e1_f_e2_instances)
            list_patterns.append(follow_pattern)

        support_overlap = len(e1_o_e2_instances.keys())
        if support_overlap >= self.minsup and support_overlap/max_supp >= self.minconf:
            overlap_pattern = Pattern([Relation.Overlaps], e1_o_e2_instances)
            list_patterns.append(overlap_pattern)

        support_contain = len(e1_c_e2_instances.keys())
        if support_contain >= self.minsup and support_contain/max_supp >= self.minconf:
            contain_pattern = Pattern([Relation.Contains], e1_c_e2_instances)
            list_patterns.append(contain_pattern)

        return list_patterns

    def FindKFrequentPatterns(self):
        level = 3
        while self.maxPatternSize == -1 or level <= self.maxPatternSize:
            level_name = 'level{}'.format(level)
            self.Nodes[level_name] = {}
            k_1_Freq = self.Nodes['level{}'.format(level-1)]
            if len(k_1_Freq) == 0:
                break
            for k_1_id_events in k_1_Freq:
                k_1_node = k_1_Freq[k_1_id_events]
                k_1_bitmap = set(k_1_node.get_bitmap())
                k_1_support = k_1_node.get_support()
                for _1_id_event in self.Nodes['level1']:
                    _1_node = self.Nodes['level1'][_1_id_event]
                    _1_bitmap = set(_1_node.get_bitmap())
                    _1_support = _1_node.get_support()

                    bitmap = k_1_bitmap.intersection(_1_bitmap)
                    maxsupp = max(k_1_support, _1_support)

                    if len(bitmap) >= self.minsup:
                        if ((k_1_support / maxsupp) < self.minconf) or ((_1_support / maxsupp) < self.minconf):
                            continue
                        else:
                            list_patterns = self.FindKPatterns(k_1_id_events, _1_id_event, level, maxsupp)
                            if list_patterns:
                                temp = list(k_1_id_events)
                                temp.append(_1_id_event)
                                k_id_events = tuple(temp)
                                node = Node(k_id_events, bitmap, maxsupp, list_patterns)
                                self.Nodes[level_name][k_id_events] = node
                                self.num_patterns += len(list_patterns)

            #self.num_patterns += len(self.Nodes[level_name])
            level += 1

    def FindKPatterns(self, k_1_id_events, _1_id_event, level, maxsupp):
        for prev_id in k_1_id_events:
            pair_id = (prev_id, _1_id_event)
            if pair_id not in self.Nodes['level2']:
                return []

        k_1_patterns = self.Nodes['level{}'.format(level-1)][k_1_id_events].get_patterns()
        # ABC <- D
        # check CD -> AD -> BD

        # phase 1: check C in ABC and CD
        last_2_id_events = (k_1_id_events[-1], _1_id_event)  # CD
        patterns_last_2_events = self.Nodes['level2'][last_2_id_events].get_patterns()

        temp_pattern_candidates = {}

        for a_pattern in k_1_patterns:
            a_pattern_bitmap = set(a_pattern.get_bitmap())

            # check support and confident between a Pattern of ABC and D
            _1_bitmap = set(self.Nodes['level1'][_1_id_event].get_bitmap())
            temp_bitmap = a_pattern_bitmap.intersection(_1_bitmap)
            if len(temp_bitmap) < self.minsup or len(temp_bitmap) / maxsupp < self.minconf:
                continue

            # find relation between a Pattern of ABC and Patterns of CD
            for last_pattern in patterns_last_2_events:
                last_pattern_bitmap = set(last_pattern.get_bitmap())

                # check support and confident
                temp_bitmap = tuple(a_pattern_bitmap.intersection(last_pattern_bitmap))
                temp_support = len(temp_bitmap)
                if temp_support < self.minsup or temp_support/maxsupp < self.minconf:
                    continue

                pattern_sID_instances = {}
                for sequenceID in temp_bitmap:
                    current_list_instances = a_pattern.get_instance_at_sequence_id(sequenceID)
                    last_list_instances = last_pattern.get_instance_at_sequence_id(sequenceID)

                    for current_instance in current_list_instances:
                        for last_instance in last_list_instances:
                            if current_instance[-1] == last_instance[0]:
                                if sequenceID in pattern_sID_instances:
                                    pattern_sID_instances[sequenceID].append(
                                        tuple(list(current_instance) + [last_instance[-1]]))
                                else:
                                    pattern_sID_instances[sequenceID] = [
                                        tuple(list(current_instance) + [last_instance[-1]])]

                new_pattern_bitmap = pattern_sID_instances.keys()
                new_pattern_support = len(new_pattern_bitmap)
                if new_pattern_support >= self.minsup and new_pattern_support/maxsupp >= self.minconf:
                    pattern_name = tuple(a_pattern.get_list_relation() + last_pattern.get_list_relation())
                    temp_pattern_candidates[pattern_name] = pattern_sID_instances

        # Phase 2:
        if len(temp_pattern_candidates) == 0:
            return []

        for index, id_event in enumerate(k_1_id_events[:-1]):
            two_events = (id_event, _1_id_event)  # AD, BD
            patterns_two_events = self.Nodes['level2'][two_events].get_patterns()

            new_pattern_candidates_update = {}

            for two_pattern in patterns_two_events:  # loop parttern in AD and check event instances in AD and current pattern candidates
                two_pattern_bitmap = set(two_pattern.get_bitmap())

                for pattern_candidate in temp_pattern_candidates:
                    current_pattern_instance = temp_pattern_candidates[pattern_candidate]
                    current_pattern_bitmap = set(current_pattern_instance.keys())

                    temp_bitmap = tuple(current_pattern_bitmap.intersection(two_pattern_bitmap))
                    temp_support = len(temp_bitmap)

                    if temp_support < self.minsup or temp_support/maxsupp < self.minconf:
                        continue

                    pattern_sID_instances = {}

                    for sequenceID in temp_bitmap:
                        current_list_instances = current_pattern_instance[sequenceID]
                        two_list_instances = two_pattern.get_instance_at_sequence_id(sequenceID)

                        for current_instance in current_list_instances:
                            for two_instance in two_list_instances:
                                if current_instance[index] == two_instance[0] and current_instance[-1] == two_instance[-1]:
                                    if sequenceID in pattern_sID_instances:
                                        pattern_sID_instances[sequenceID].append(current_instance)
                                    else:
                                        pattern_sID_instances[sequenceID] = [current_instance]

                    new_pattern_bitmap = pattern_sID_instances.keys()
                    new_pattern_support = len(new_pattern_bitmap)
                    if new_pattern_support >= self.minsup and new_pattern_support/maxsupp >= self.minconf:
                        pattern_name = tuple(list(pattern_candidate) + two_pattern.get_list_relation())
                        new_pattern_candidates_update[pattern_name] = pattern_sID_instances

            temp_pattern_candidates = new_pattern_candidates_update  # update temp pattern for next check

        list_pattern = []

        for pattern_name, pattern_sID_instances in temp_pattern_candidates.items():
            tmp = Pattern(pattern_name, pattern_sID_instances)
            list_pattern.append(tmp)

        patterns = list_pattern

        return patterns
