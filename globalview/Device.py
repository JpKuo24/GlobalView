#  Representation of a infrastructure device, which is internal node in the graph
class Device(object):
    def __init__(self,ssid, state, devicetype, heartbeat):
        self._deviceId = ssid
        self._state = state
        self._deviceType = devicetype
        self._heartbeat = heartbeat

    def get_deviceid(self):
        return self._deviceId

    def get_devicetype(self):
        return self._deviceType

    def get_heartbeat(self):
        return self._heartbeat

    def get_state(self):
        return self._state

    def set_deviceid(self, deviceid):
        self._deviceId = deviceid

    def set_heartbeat(self, heartbeat):
        self._heartbeat = heartbeat

    def set_state(self, state):
        self._state = state


class Stopper(Device):
    def __init__(self, ssid, state, heartbeat):
        super(Stopper, self).__init__(ssid, state, 'Stopper', heartbeat)


class Robot(Device):
    def __init__(self,ssid, state, heartbeat, program):
        super(Robot, self).__init__(ssid, state, 'Robot', heartbeat)
        self._program = program

