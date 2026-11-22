def required(replica_count: int) -> int:
    """Majority of active RMs; requires at least 1 RM to confirm."""
    if replica_count <= 0:
        return 1
    return (replica_count // 2) + 1


def handle_result(self, message, addr):
    if self.leader != self.ip or message.payload.get('epoch') != self.epoch:
        return
    fields = message.payload
    operation = self.pending.get((self.epoch, fields.get('seq')))
    if operation is not None and operation.acknowledge(
            addr[0], fields['epoch'], fields.get('seq'), fields.get('total'), self.view):
        self.cv.notify_all()
