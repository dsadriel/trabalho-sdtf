"""RM workers apply in total order and cache results until the FE confirms a prefix."""
import threading

from common.config import MAX_INFLIGHT_OPERATIONS


def discard_results(self, committed):
    if type(committed) is not int or committed < 0:
        return
    self.replica_committed = max(self.replica_committed, min(committed, self.state.num_reqs))
    for sequence in list(self.result_cache):
        if sequence <= self.replica_committed:
            self.result_cache.pop(sequence)


def send_result(self, source, epoch, sequence, total):
    try:
        self.send(source, 'RESULT', seq=sequence, total=total, epoch=epoch)
    except OSError as exc:
        self.interface.log('RESULT send failed', sequence, exc)


def valid_worker(self, source, epoch, version):
    return (self.leader == source and self.leader != self.ip and self.ready
            and self.epoch == epoch and self.replica_version == version)


def handle_op(self, message, addr):
    source = addr[0]
    fields = message.payload
    epoch = fields['epoch']
    # Só aceita OP se este nó for RM, vier do líder atual e pertencer à mesma época.
    if self.leader == self.ip or source != self.leader or epoch != self.epoch:
        return
    if not self.ready:
        # Ainda sincronizando: reforça pedido de snapshot ao FE.
        self.send(source, 'JOIN', snap=self.state.snapshot())
        return
    operation = tuple(fields['op'])
    if len(operation) != 6 or type(operation[0]) is not int or operation[0] < 1:
        return
    sequence = operation[0]
    # Coleta de lixo do result_cache até o prefixo global já confirmado pelo FE.
    discard_results(self, fields.get('committed', 0))
    # Operação já aplicada antes (ex.: RESULT anterior perdido no UDP): reenvia do cache.
    if sequence <= self.state.num_reqs:
        cached = self.result_cache.get(sequence)
        if cached is not None and cached[0] == operation:
            send_result(self, source, epoch, sequence, cached[1])
        return
    if sequence in self.buffer:
        return  # Já existe um worker aguardando para aplicar esta sequência.
    # Se o RM ficou mais de uma janela inteira atrás, ressincroniza via snapshot.
    if sequence > self.state.num_reqs + MAX_INFLIGHT_OPERATIONS:
        self.send(source, 'JOIN', snap=self.state.snapshot())
        return
    if len(self.replica_jobs) >= MAX_INFLIGHT_OPERATIONS:
        return  # Backpressure: o FE retransmite a OP sem criar threads ilimitadas.
    # replica_version invalida automaticamente workers antigos caso um snapshot seja instalado.
    version = self.replica_version
    job = (version, epoch, sequence)
    self.buffer[sequence] = operation
    self.replica_jobs.add(job)
    worker = threading.Thread(target=apply_operation,
                              args=(self, source, epoch, version, operation), daemon=True)
    try:
        worker.start()
    except RuntimeError as exc:
        self.replica_jobs.discard(job)
        self.buffer.pop(sequence, None)
        self.cv.notify_all()
        self.interface.log('RM worker not started', sequence, exc)


def apply_operation(self, source, epoch, version, operation):
    sequence, session, request_id, value, ip, port = operation
    job = (version, epoch, sequence)
    result = None
    try:
        with self.cv:
            # Ordenação total no RM: aguarda em cv.wait() até que esta sequência seja
            # exatamente a próxima esperada pelo acumulador (state.num_reqs + 1).
            while valid_worker(self, source, epoch, version) and sequence != self.state.num_reqs + 1:
                if sequence <= self.state.num_reqs:
                    return
                self.cv.wait()  # Libera o lock para que a sequência anterior possa executar.
            # Aborta se o líder, a época ou replica_version mudaram durante a espera.
            if not valid_worker(self, source, epoch, version):
                return
            status, number, total = self.state.process_request(ip, request_id, value, port, session=session)
            if status != 'NORMAL' or number != sequence:
                self.interface.log('RM state conflict', sequence, status)
                self.send(source, 'JOIN', snap=self.state.snapshot())
                return
            self.result_cache[sequence] = (operation, total)  # Salva para retransmissões do FE.
            result = total
            self.buffer.pop(sequence, None)
            self.interface.log('RM applied', sequence, result)
            self.cv.notify_all()  # Acorda a thread da próxima sequência (seq + 1).
        # Envio de rede fora da seção crítica; resultados de épocas antigas são rejeitados pelo FE.
        send_result(self, source, epoch, sequence, result)
    finally:
        with self.cv:
            self.replica_jobs.discard(job)
            if self.replica_version == version and self.buffer.get(sequence) is operation:
                self.buffer.pop(sequence)
            self.cv.notify_all()
