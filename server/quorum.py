def required(replica_count: int) -> int:
    """Maioria simples dos RMs ativos; com 0 RMs exige 1 (inatingível, bloqueia confirmações)."""
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
        self.cv.notify_all()  # Acorda handle_request() para reavaliar quórum e prefixo.
