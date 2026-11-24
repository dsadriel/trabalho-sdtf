#!/usr/bin/env python3
"""End-to-end local test suite with real server and client processes.

Runs multiple `servidor.py --local` and `cliente.py --local` processes over real
loopback UDP and IPv4 multicast sockets, generating random integer workloads in
memory (no external input files required) and validating exact sums and cluster
fault-tolerance events.
"""
from __future__ import annotations

import argparse
import random
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
ACK_PATTERN = re.compile(
    r"server\s+(\S+)\s+id_req\s+(\d+)\s+value\s+(\d+)\s+num_reqs\s+(\d+)\s+total_sum\s+(\d+)$"
)
COORDINATOR_READY_PATTERN = re.compile(r"COORDINATOR ready (\S+)")
RM_JOINED_PATTERN = re.compile(r"RM joined (\S+)")
CONTROL_KEYWORDS = (
    "COORDINATOR",
    "RM joined",
    "RM timeout",
    "FE timeout",
    "ELECTION",
    "HISTORY GAP",
    "STATE CONFLICT",
    "RECOVERY",
    "quorum",
)


@dataclass(frozen=True)
class Workload:
    client_id: int
    values: list[int]
    expected_sum: int

    @property
    def line_count(self) -> int:
        return len(self.values)


def generate_workloads(
    num_clients: int, count_per_client: int, max_value: int, seed: int | None
) -> list[Workload]:
    if num_clients < 1:
        raise ValueError("--clients deve ser >= 1")
    if count_per_client < 1:
        raise ValueError("--count deve ser >= 1")
    if max_value < 1:
        raise ValueError("--max-value deve ser >= 1")

    rng = random.Random(seed)
    workloads: list[Workload] = []
    for client_id in range(1, num_clients + 1):
        values = [rng.randint(1, max_value) for _ in range(count_per_client)]
        workloads.append(Workload(client_id, values, sum(values)))
    return workloads


def check_loopback_ips(count: int, port: int) -> None:
    missing: list[str] = []
    for index in range(1, count + 1):
        ip = f"127.0.0.{index}"
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            try:
                sock.bind((ip, port))
            except OSError:
                missing.append(ip)
    if missing:
        cmds = "\n".join(f"  sudo ifconfig lo0 alias {ip} up" for ip in missing)
        raise RuntimeError(
            "Os seguintes IPs de loopback não estão disponíveis para bind local:\n"
            f"{', '.join(missing)}\n"
            "No macOS, habilite-os antes de rodar a suíte:\n"
            f"{cmds}"
        )


class ServerProcess:
    def __init__(self, port: int, label: str) -> None:
        self.port = port
        self.label = label
        self.ip: str | None = None
        self.events: list[str] = []
        self.stderr_lines: list[str] = []
        self._cv = threading.Condition()
        self.proc = subprocess.Popen(
            [sys.executable, "-u", str(ROOT_DIR / "servidor.py"), str(port), "--local"],
            cwd=str(ROOT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._stdout_thread = threading.Thread(
            target=self._drain_stdout, name=f"stdout-{label}", daemon=True
        )
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr, name=f"stderr-{label}", daemon=True
        )
        self._stdout_thread.start()
        self._stderr_thread.start()

    def _drain_stdout(self) -> None:
        assert self.proc.stdout is not None
        for raw in self.proc.stdout:
            line = raw.rstrip("\r\n")
            if any(keyword in line for keyword in CONTROL_KEYWORDS):
                with self._cv:
                    self.events.append(line)
                    ready_match = COORDINATOR_READY_PATTERN.search(line)
                    if ready_match and self.ip is None:
                        self.ip = ready_match.group(1)
                    self._cv.notify_all()

    def _drain_stderr(self) -> None:
        assert self.proc.stderr is not None
        for raw in self.proc.stderr:
            line = raw.rstrip("\r\n")
            with self._cv:
                self.stderr_lines.append(line)
                if len(self.stderr_lines) > 50:
                    self.stderr_lines.pop(0)
                self._cv.notify_all()

    def wait_event(self, substring: str, timeout: float = 15.0) -> str:
        deadline = time.monotonic() + timeout
        with self._cv:
            while True:
                for event in self.events:
                    if substring in event:
                        return event
                if self.proc.poll() is not None:
                    raise RuntimeError(
                        f"Servidor {self.label} encerrou inesperadamente (rc={self.proc.returncode}). "
                        f"stderr={self.stderr_lines}"
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Timeout aguardando '{substring}' em {self.label}. "
                        f"Eventos recentes: {self.events[-10:]}"
                    )
                self._cv.wait(timeout=remaining)

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=3)


class ClientProcess:
    def __init__(self, port: int, workload: Workload, label: str) -> None:
        self.port = port
        self.workload = workload
        self.label = label
        self.acks_received = 0
        self.last_num_reqs = 0
        self.last_total_sum = 0
        self.last_server: str | None = None
        self.servers_seen: set[str] = set()
        self.stderr_lines: list[str] = []
        self._lock = threading.Lock()
        self.proc = subprocess.Popen(
            [sys.executable, "-u", str(ROOT_DIR / "cliente.py"), str(port), "--local"],
            cwd=str(ROOT_DIR),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._stdin_thread = threading.Thread(
            target=self._feed_stdin, name=f"stdin-{label}", daemon=True
        )
        self._stdout_thread = threading.Thread(
            target=self._drain_stdout, name=f"stdout-{label}", daemon=True
        )
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr, name=f"stderr-{label}", daemon=True
        )
        self._stdout_thread.start()
        self._stderr_thread.start()
        self._stdin_thread.start()

    def _feed_stdin(self) -> None:
        assert self.proc.stdin is not None
        try:
            chunk: list[str] = []
            for value in self.workload.values:
                chunk.append(f"{value}\n")
                if len(chunk) >= 1024:
                    self.proc.stdin.write("".join(chunk))
                    chunk.clear()
            if chunk:
                self.proc.stdin.write("".join(chunk))
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        finally:
            try:
                self.proc.stdin.close()
            except OSError:
                pass

    def _drain_stdout(self) -> None:
        assert self.proc.stdout is not None
        for raw in self.proc.stdout:
            line = raw.rstrip("\r\n")
            match = ACK_PATTERN.search(line)
            if match:
                server_ip = match.group(1)
                num_reqs = int(match.group(4))
                total_sum = int(match.group(5))
                with self._lock:
                    self.acks_received += 1
                    self.last_server = server_ip
                    self.servers_seen.add(server_ip)
                    self.last_num_reqs = num_reqs
                    self.last_total_sum = total_sum

    def _drain_stderr(self) -> None:
        assert self.proc.stderr is not None
        for raw in self.proc.stderr:
            line = raw.rstrip("\r\n")
            with self._lock:
                self.stderr_lines.append(line)
                if len(self.stderr_lines) > 50:
                    self.stderr_lines.pop(0)

    def snapshot(self) -> tuple[int, int, int, set[str]]:
        with self._lock:
            return (
                self.acks_received,
                self.last_num_reqs,
                self.last_total_sum,
                set(self.servers_seen),
            )

    def wait_done(self) -> int:
        rc = self.proc.wait()
        self._stdin_thread.join(timeout=5)
        self._stdout_thread.join(timeout=5)
        self._stderr_thread.join(timeout=5)
        return rc

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                pass


class LocalCluster:
    def __init__(self, port: int) -> None:
        self.port = port
        self.servers: list[ServerProcess] = []
        self.leader: ServerProcess | None = None

    def start_initial(self, count: int) -> None:
        if count < 1:
            raise ValueError("count deve ser >= 1")
        first = ServerProcess(self.port, "S1")
        self.servers.append(first)
        first.wait_event("COORDINATOR ready", timeout=15.0)
        self.leader = first
        for idx in range(2, count + 1):
            self.add_replica(f"S{idx}")

    def add_replica(self, label: str | None = None) -> ServerProcess:
        assert self.leader is not None
        name = label or f"S{len(self.servers) + 1}"
        known_joins = sum(1 for ev in self.leader.events if "RM joined" in ev)
        replica = ServerProcess(self.port, name)
        self.servers.append(replica)
        deadline = time.monotonic() + 15.0
        with self.leader._cv:
            while True:
                joins = [ev for ev in self.leader.events if "RM joined" in ev]
                if len(joins) > known_joins:
                    match = RM_JOINED_PATTERN.search(joins[-1])
                    if match:
                        replica.ip = match.group(1)
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Timeout aguardando {name} ingressar no líder {self.leader.label}."
                    )
                self.leader._cv.wait(timeout=remaining)
        return replica

    def stop_all(self) -> None:
        for server in self.servers:
            server.stop()


def wait_clients_with_progress(
    clients: list[ClientProcess],
    expected_total_reqs: int,
    expected_total_sum: int,
    started_at: float,
    hook=None,
) -> tuple[int, int, float]:
    """Waits for all clients while printing live progress and running an optional hook."""
    last_print = 0.0
    hook_done = hook is None

    while True:
        alive = [c for c in clients if c.proc.poll() is None]
        acks = sum(c.snapshot()[0] for c in clients)
        now = time.monotonic()

        if not hook_done:
            hook_done = bool(hook(acks))

        if now - last_print >= 2.0 or not alive:
            elapsed = max(now - started_at, 1e-6)
            pct = (acks / expected_total_reqs * 100.0) if expected_total_reqs else 100.0
            rate = acks / elapsed
            print(
                f"  [progresso] {acks:,}/{expected_total_reqs:,} ({pct:5.1f}%) | "
                f"{elapsed:6.1f}s | {rate:,.0f} req/s",
                flush=True,
            )
            last_print = now

        if not alive:
            break
        time.sleep(0.2)

    elapsed = time.monotonic() - started_at
    for client in clients:
        rc = client.wait_done()
        if rc != 0:
            raise AssertionError(
                f"{client.label} falhou com rc={rc}. stderr={client.stderr_lines}"
            )
        acks, _, _, _ = client.snapshot()
        if acks != client.workload.line_count:
            raise AssertionError(
                f"{client.label} recebeu {acks} ACKs, esperado {client.workload.line_count}"
            )

    snapshots = [c.snapshot() for c in clients]
    best = max(snapshots, key=lambda item: item[1])
    final_num_reqs, final_total_sum = best[1], best[2]
    if final_num_reqs != expected_total_reqs:
        raise AssertionError(
            f"num_reqs final incorreto: obtido {final_num_reqs}, esperado {expected_total_reqs}"
        )
    if final_total_sum != expected_total_sum:
        raise AssertionError(
            f"total_sum final incorreto: obtido {final_total_sum}, esperado {expected_total_sum}"
        )
    return final_num_reqs, final_total_sum, elapsed


def scenario_normal_load(workloads: list[Workload], port: int) -> None:
    cluster = LocalCluster(port)
    clients: list[ClientProcess] = []
    try:
        cluster.start_initial(4)  # 1 FE + 3 RMs
        started = time.monotonic()
        clients = [ClientProcess(port, w, f"C{w.client_id}") for w in workloads]
        expected_reqs = sum(w.line_count for w in workloads)
        expected_sum = sum(w.expected_sum for w in workloads)
        num_reqs, total_sum, elapsed = wait_clients_with_progress(
            clients, expected_reqs, expected_sum, started
        )
        print(
            f"  [OK] normal_load: num_reqs={num_reqs:,}, total_sum={total_sum:,}, "
            f"tempo={elapsed:.1f}s ({num_reqs / elapsed:,.0f} req/s)",
            flush=True,
        )
    finally:
        for client in clients:
            client.stop()
        cluster.stop_all()


def scenario_rm_crash_majority(workloads: list[Workload], port: int) -> None:
    cluster = LocalCluster(port)
    clients: list[ClientProcess] = []
    try:
        cluster.start_initial(4)  # 1 FE + 3 RMs (quorum = 2)
        victim = cluster.servers[-1]
        started = time.monotonic()
        clients = [ClientProcess(port, w, f"C{w.client_id}") for w in workloads]
        expected_reqs = sum(w.line_count for w in workloads)
        expected_sum = sum(w.expected_sum for w in workloads)
        trigger = min(500, max(1, expected_reqs // 4))

        def inject_rm_crash(acks: int) -> bool:
            if acks < trigger:
                return False
            print(f"  [evento] Matando RM {victim.label} ({victim.ip}) com SIGKILL...", flush=True)
            victim.kill()
            assert cluster.leader is not None
            event = cluster.leader.wait_event(f"RM timeout {victim.ip}", timeout=10.0)
            print(f"  [evento] Detectado no FE: {event}", flush=True)
            return True

        num_reqs, total_sum, elapsed = wait_clients_with_progress(
            clients, expected_reqs, expected_sum, started, hook=inject_rm_crash
        )
        print(
            f"  [OK] rm_crash_majority: num_reqs={num_reqs:,}, total_sum={total_sum:,}, "
            f"tempo={elapsed:.1f}s",
            flush=True,
        )
    finally:
        for client in clients:
            client.stop()
        cluster.stop_all()


def scenario_fe_failover_ring(workloads: list[Workload], port: int) -> None:
    cluster = LocalCluster(port)
    clients: list[ClientProcess] = []
    try:
        cluster.start_initial(4)  # 1 FE (127.0.0.1) + 3 RMs (127.0.0.2..4)
        old_fe = cluster.servers[0]
        expected_new_fe = cluster.servers[-1]  # maior IP (127.0.0.4)
        started = time.monotonic()
        clients = [ClientProcess(port, w, f"C{w.client_id}") for w in workloads]
        expected_reqs = sum(w.line_count for w in workloads)
        expected_sum = sum(w.expected_sum for w in workloads)
        trigger = min(500, max(1, expected_reqs // 4))

        def inject_fe_crash(acks: int) -> bool:
            if acks < trigger:
                return False
            print(f"  [evento] Matando FE {old_fe.label} ({old_fe.ip}) com SIGKILL...", flush=True)
            old_fe.kill()
            event = expected_new_fe.wait_event("COORDINATOR ready", timeout=15.0)
            print(f"  [evento] Novo FE eleito: {event}", flush=True)
            cluster.leader = expected_new_fe
            return True

        num_reqs, total_sum, elapsed = wait_clients_with_progress(
            clients, expected_reqs, expected_sum, started, hook=inject_fe_crash
        )
        seen_servers = set().union(*(c.snapshot()[3] for c in clients))
        if expected_new_fe.ip not in seen_servers:
            raise AssertionError(
                f"Clientes não redirecionaram para o novo FE {expected_new_fe.ip}: {seen_servers}"
            )
        print(
            f"  [OK] fe_failover_ring: num_reqs={num_reqs:,}, total_sum={total_sum:,}, "
            f"novo_fe={expected_new_fe.ip}, tempo={elapsed:.1f}s",
            flush=True,
        )
    finally:
        for client in clients:
            client.stop()
        cluster.stop_all()


def scenario_rm_join_under_load(workloads: list[Workload], port: int) -> None:
    cluster = LocalCluster(port)
    clients: list[ClientProcess] = []
    try:
        cluster.start_initial(3)  # 1 FE + 2 RMs
        started = time.monotonic()
        clients = [ClientProcess(port, w, f"C{w.client_id}") for w in workloads]
        expected_reqs = sum(w.line_count for w in workloads)
        expected_sum = sum(w.expected_sum for w in workloads)
        trigger = min(500, max(1, expected_reqs // 4))

        def inject_rm_join(acks: int) -> bool:
            if acks < trigger:
                return False
            print("  [evento] Subindo novo RM (S4) durante a carga...", flush=True)
            newcomer = cluster.add_replica("S4")
            print(f"  [evento] Novo RM ingressou: {newcomer.label} ({newcomer.ip})", flush=True)
            return True

        num_reqs, total_sum, elapsed = wait_clients_with_progress(
            clients, expected_reqs, expected_sum, started, hook=inject_rm_join
        )
        print(
            f"  [OK] rm_join_under_load: num_reqs={num_reqs:,}, total_sum={total_sum:,}, "
            f"tempo={elapsed:.1f}s",
            flush=True,
        )
    finally:
        for client in clients:
            client.stop()
        cluster.stop_all()


def scenario_zero_rm_block_and_recover(workloads: list[Workload], port: int) -> None:
    cluster = LocalCluster(port)
    clients: list[ClientProcess] = []
    workload = workloads[0]
    try:
        cluster.start_initial(2)  # 1 FE + 1 RM
        only_rm = cluster.servers[1]
        started = time.monotonic()
        clients = [ClientProcess(port, workload, f"C{workload.client_id}")]
        trigger = min(200, max(1, workload.line_count // 4))

        def inject_zero_rm_and_recover(acks: int) -> bool:
            if acks < trigger:
                return False
            print(f"  [evento] Matando único RM {only_rm.label} ({only_rm.ip})...", flush=True)
            only_rm.kill()
            assert cluster.leader is not None
            event = cluster.leader.wait_event("quorum unavailable (0 RMs)", timeout=10.0)
            print(f"  [evento] Bloqueio confirmado no FE: {event}", flush=True)
            acks_after_block = clients[0].snapshot()[0]
            time.sleep(1.0)
            acks_still_blocked = clients[0].snapshot()[0]
            if acks_still_blocked != acks_after_block:
                raise AssertionError(
                    f"FE confirmou pedidos sem RMs ativos ({acks_after_block} -> {acks_still_blocked})"
                )
            print("  [evento] Subindo novo RM para restaurar quórum...", flush=True)
            recovered = cluster.add_replica("S3")
            print(f"  [evento] Quórum restaurado com {recovered.label} ({recovered.ip})", flush=True)
            return True

        num_reqs, total_sum, elapsed = wait_clients_with_progress(
            clients, workload.line_count, workload.expected_sum, started,
            hook=inject_zero_rm_and_recover,
        )
        print(
            f"  [OK] zero_rm_block_and_recover: num_reqs={num_reqs:,}, total_sum={total_sum:,}, "
            f"tempo={elapsed:.1f}s",
            flush=True,
        )
    finally:
        for client in clients:
            client.stop()
        cluster.stop_all()


SCENARIOS = {
    "normal_load": scenario_normal_load,
    "rm_crash_majority": scenario_rm_crash_majority,
    "fe_failover_ring": scenario_fe_failover_ring,
    "rm_join_under_load": scenario_rm_join_under_load,
    "zero_rm_block_and_recover": scenario_zero_rm_block_and_recover,
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Suíte E2E local com múltiplos servidores e clientes gerando números automaticamente."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Quantidade de números aleatórios gerados por cliente (padrão: 1000).",
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=3,
        help="Número de clientes concorrentes (padrão: 3).",
    )
    parser.add_argument(
        "--max-value",
        type=int,
        default=50,
        help="Valor máximo de cada inteiro gerado, entre 1 e max-value (padrão: 50).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Semente opcional do gerador aleatório para reproduzir a mesma carga.",
    )
    parser.add_argument(
        "--scenarios",
        nargs="+",
        choices=list(SCENARIOS.keys()),
        default=list(SCENARIOS.keys()),
        help="Cenários a executar (padrão: todos).",
    )
    parser.add_argument(
        "--base-port",
        type=int,
        default=4100,
        help="Porta UDP base (cada cenário usa base_port + índice).",
    )
    args = parser.parse_args()

    def handle_sigint(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_sigint)

    workloads = generate_workloads(args.clients, args.count, args.max_value, args.seed)
    print(
        f"Carga gerada automaticamente: {args.clients} cliente(s) x {args.count:,} números "
        f"(1..{args.max_value}, seed={args.seed})",
        flush=True,
    )
    for w in workloads:
        print(f"  - Cliente C{w.client_id}: {w.line_count:,} números, soma esperada = {w.expected_sum:,}", flush=True)
    print(
        f"Total combinado: {sum(w.line_count for w in workloads):,} requisições, "
        f"soma esperada = {sum(w.expected_sum for w in workloads):,}\n",
        flush=True,
    )

    check_loopback_ips(4, args.base_port)

    failures: list[tuple[str, Exception]] = []
    for index, name in enumerate(args.scenarios):
        port = args.base_port + index
        print(f"=== Cenário [{index + 1}/{len(args.scenarios)}]: {name} (porta {port}) ===", flush=True)
        try:
            SCENARIOS[name](workloads, port)
        except Exception as exc:
            failures.append((name, exc))
            print(f"  [FALHA] {name}: {exc}", file=sys.stderr, flush=True)

    print("\n=== Resumo Final ===", flush=True)
    print(
        f"Passaram: {len(args.scenarios) - len(failures)}/{len(args.scenarios)}",
        flush=True,
    )
    if failures:
        for name, exc in failures:
            print(f"  - {name}: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
