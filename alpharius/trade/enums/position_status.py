from enum import Enum


class PositionStatus(Enum):
    ACTIVE = 1
    PENDING = 2
    CLOSED = 3

    def __str__(self):
        return self.name
