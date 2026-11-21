def required(replica_count: int) -> int:
    """Every participating RM still in the view must confirm."""
    return replica_count


def handle_result(self, message, addr):
    if self.leader != self.ip or message.payload.get('epoch') != self.epoch:
        return
    fields = message.payload
    operation = self.pending.get((self.epoch, fields.get('seq')))
    if operation is not None and operation.acknowledge(
            addr[0], fields['epoch'], fields.get('seq'), fields.get('total'), self.view):
        self.cv.notify_all()
