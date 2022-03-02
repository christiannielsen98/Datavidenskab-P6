from enum import Enum


class Relation(Enum):
    Follows = 1
    Contains = 2
    Overlaps = 3

    def __str__(self) -> str:
        obj = {
            Relation.Follows: '->',
            Relation.Contains: '>',
            Relation.Overlaps: '|'
        }
        return obj[self]
