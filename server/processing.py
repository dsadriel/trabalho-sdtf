"""Concurrent FE requests with a bounded pipeline and ordered confirmation."""
import threading
from dataclasses import dataclass, field
from time import monotonic as CLOCK

from common.config import MULTICAST_GROUP, MAX_INFLIGHT_OPERATIONS, OP_RETRY_INTERVAL
from common.protocol import Message
from server.operations import PendingOperation
from server.state import ServerState


@dataclass
class RequestTask:
    key: tuple
    epoch: str
    addresses: set = field(default_factory=set)
    response: Message | None = None


def client_key(message, address):
    session = message.payload.get('session')
    return session if session is not None else address


def send_reply(self, address, response):
    try:
        self.sock.sendto(response.encode(), address)
    except OSError as exc:
        # The committed state remains available for the client's retry.
        self.interface.log('ACK send failed', address, exc)


def respond(self, task, response):
    task.response = response
    for address in tuple(task.addresses):
        send_reply(self, address, response)


def start_request(self, message, address):
    """One worker per unique request; retries attach to the existing task."""
    with self.cv:
        key = (self.epoch, client_key(message, address), message.id_req)
        task = self.request_workers.get(key)
        if task is not None:
            if task.response is not None:
                send_reply(self, address, task.response)
            else:
                task.addresses.add(address)
            return
        if len(self.request_workers) >= MAX_INFLIGHT_OPERATIONS:
            return  # UDP retry provides backpressure without unbounded thread creation.
        task = RequestTask(key, self.epoch, {address})
        self.request_workers[key] = task
        worker = threading.Thread(target=handle_request,
                                  args=(self.sock, address, message, self.state),
                                  kwargs=dict(interface=self.interface, show_port=self.show_port,
                                              coordinator=self, epoch=self.epoch, task=task), daemon=True)
        try:
            worker.start()
        except RuntimeError as exc:
            self.request_workers.pop(key)
            self.cv.notify_all()
            self.interface.log('REQUEST worker not started', exc)


def active(self, epoch):
    return self.leader == self.ip and self.epoch == epoch and self.ready


def handle_request(sock, addr, msg: Message, state: ServerState,
                   interface=None, show_port=False, *, coordinator, epoch, task) -> None:
    self = coordinator
    operation = None
    log_result = None
    session = msg.payload.get('session')
    try:
        with self.cv:
            while active(self, epoch):
                # Garante que existe um ClientRecord (na primeira vez cria com last_req=0).
                client = state.register_client(addr[0], addr[1], session=session)

                # Portão 1: duplicata já confirmada anteriormente — devolve o ACK salvo sem replicar.
                if msg.id_req <= client.last_req:
                    respond(self, task, Message.ack(client.last_req, client.last_num_reqs, client.last_total_sum))
                    log_result = ('DUPLICATE', client.last_num_reqs, client.last_total_sum)
                    return

                # Portão 2: ordem por cliente (mesma sessão).
                # Se existe outro worker ativo da mesma sessão com id_req menor, aguarda sua conclusão;
                # clientes distintos possuem chaves diferentes e avançam em paralelo.
                preceding = any(key[0] == epoch and key[1] == task.key[1] and key[2] < msg.id_req
                                for key in self.request_workers)
                if preceding:
                    self.cv.wait()
                    continue

                # Portão 3: lacuna de histórico (HISTORY_GAP).
                # Só chegamos aqui sem pedidos anteriores da mesma sessão em voo; se id_req saltou,
                # recusa sem somar para não devolver uma soma incompleta ao cliente.
                if msg.id_req > client.last_req + 1:
                    respond(self, task, Message('HISTORY_GAP', session=session, id_req=msg.id_req,
                            last_req=client.last_req, num_reqs=state.num_reqs, total_sum=state.total_sum))
                    self.interface.log('HISTORY GAP', session, 'requested', msg.id_req,
                                       'last_req', client.last_req, 'seq', state.num_reqs)
                    return

                # Portão 4: barreira de ingresso de novo RM, mínimo de 1 RM ativo e limite do pipeline.
                if self.join_waiters or len(self.view) <= 1 or len(self.pending) >= MAX_INFLIGHT_OPERATIONS:
                    self.cv.wait()
                    continue

                # Admissão: sincroniza contadores especulativos se o pipeline estava vazio e reserva seq.
                if not self.pending:
                    self.next_sequence, self.reserved_total = state.num_reqs, state.total_sum
                self.next_sequence += 1
                self.reserved_total += msg.value
                operation = PendingOperation(epoch, self.next_sequence, session, msg.id_req,
                    msg.value, addr, self.reserved_total, frozenset(set(self.view) - {self.ip}))
                self.pending[operation.key] = operation
                break
            if operation is None:
                return
            retry_at = 0.0
            while active(self, epoch) and not operation.cancelled:
                confirmed = operation.confirmed(self.view)
                # Commit ordenado por prefixo: só aplica em ServerState e envia ACK quando
                # atingir quórum E for exatamente a próxima sequência esperada (num_reqs + 1).
                if confirmed and operation.sequence == state.num_reqs + 1:
                    status, number, total = state.process_request(
                        addr[0], msg.id_req, msg.value, addr[1], session=session)
                    if status != 'NORMAL' or number != operation.sequence or total != operation.expected_total:
                        self.ready = False
                        self.cancel_work()
                        self.interface.log('STATE CONFLICT during confirmation', operation.sequence)
                        return
                    self.pending.pop(operation.key)
                    # Responde ao endereço original e a eventuais retransmissões acopladas à mesma tarefa.
                    respond(self, task, Message.ack(msg.id_req, number, total))
                    log_result = (status, number, total)
                    self.cv.notify_all()  # Acorda a thread de seq + 1.
                    if not self.pending:
                        from server.recovery import resume_joins
                        resume_joins(self)
                        self.announce()  # Divulga o prefixo confirmado para os RMs limparem o cache.
                    return
                # Enquanto não atingir quórum, dissemina/retransmite OP em multicast a cada 0,2s.
                if not confirmed and CLOCK() >= retry_at:
                    try:
                        self.send(MULTICAST_GROUP, 'OP', epoch=epoch, op=operation.packet,
                                  committed=state.num_reqs)
                    except OSError as exc:
                        self.interface.log('OP send failed', operation.sequence, exc)
                    retry_at = CLOCK() + OP_RETRY_INTERVAL
                # Libera self.cv durante a espera (sem timeout se já atingiu quórum e só aguarda seq - 1).
                self.cv.wait(None if confirmed else max(0.0, retry_at - CLOCK()))
    finally:
        with self.cv:
            # Limpeza por identidade (`is`) para não remover um worker recém-criado em retentativa rápida.
            if operation is not None and self.pending.get(operation.key) is operation:
                operation.cancelled = True
                self.pending.pop(operation.key)
            if self.request_workers.get(task.key) is task:
                self.request_workers.pop(task.key)
            self.cv.notify_all()
        if log_result is not None and interface:
            status, number, total = log_result
            display = f'{addr[0]}:{addr[1]}' if show_port else addr[0]
            printer = interface.print_duplicate if status == 'DUPLICATE' else interface.print_request
            printer(display, msg.id_req, msg.value, number, total)


def handle_exit(sock, addr, state: ServerState, interface=None, show_port=False):
    """Retain the history so delayed UDP packets remain idempotent."""
    if interface:
        display = f'{addr[0]}:{addr[1]}' if show_port else addr[0]
        interface.log('EXIT', display)
