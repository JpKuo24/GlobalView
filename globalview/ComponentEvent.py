from enum import Enum, unique


class AbstractEvent(object):
    def __init__(self, eventtype, subject, ssid, time = 0):
        self._eventype = eventtype
        self._subject = subject
        self._ssid = ssid
        self._time = time

    def get_time(self):
        return self._time

    def get_type(self):
        return self._eventype

    def get_subject(self):
        return self._subject

    def get_ssid(self):
        return self._ssid


@unique
class EventType(Enum):
    ADDED = 'add'  # Signifies that a new component is detected

    ERROR = 'error'  # Signifies that the component failure happened,
                       # temporarily delete the component as well as the links

    UPDATED = 'update'  # Signifies that update the information of component
                        # e.g. some variables like location changed

    IDLING = 'idle'   # Signifies that there is no job in the process

    REMOVED = 'remove'  # Signifies that a component is removed from plant permanently

    RECOVER = 'recover' # Signifies that a component is recovered from error


class CncEvent(AbstractEvent):
    def __init__(self, eventtype, ssid, time):
        super(CncEvent, self).__init__(eventtype, 'cnc', ssid, time)


class StopperEvent(AbstractEvent):
    def __init__(self, eventtype, ssid, time):
        super(StopperEvent,self).__init__(eventtype, 'stopper', ssid, time)


class RobotEvent(AbstractEvent):
    def __init__(self, eventtype, ssid, time):
        super(RobotEvent,self).__init__(eventtype, 'rebot', ssid, time)
