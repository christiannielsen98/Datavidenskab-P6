from .Relation import *
from datetime import *


class Utils:
    def check_relation(time_a, time_b, label_a, label_b, epsilon, min_overlap):
        relation = None
        if time_a.start == time_b.start:
            if time_a.end == time_b.end:
                if label_a >= label_b:
                    return relation
        elif time_a.start > time_b.start:
            return relation

        if label_a != label_b:
            if time_a.end - epsilon <= time_b.start:
                relation = Relation.Follows
            elif (time_a.start <= time_b.start) and (time_a.end + epsilon >= time_b.end):
                relation = Relation.Contains
            elif (time_a.start < time_b.start) and (time_a.end - epsilon < time_b.end) and (time_a.end - time_b.start >= min_overlap - epsilon):
                relation = Relation.Overlaps
        else:
            if time_a.end - epsilon <= time_b.start:
                relation = Relation.Follows
        return relation

    def checkArgs(minsup, minconf, epsilon, minoverlap, maxpatternsize):
        if minsup <= 0 or minsup > 1:
            raise ValueError("minsup must be in range (0,1]")
        if minconf <= 0 or minconf > 1:
            raise ValueError("minconf must be in range (0,1]")
        if 0 > epsilon:
            raise ValueError("epsilon must be larger or equal 0")
        if 0 >= minoverlap:
            raise ValueError("minoverlap must be larger 0")
        if 2 > maxpatternsize:
            if maxpatternsize != -1:
                raise ValueError("maxpatternsize must be larger or equal 2")
