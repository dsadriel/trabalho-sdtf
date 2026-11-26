# Sistemas Distribuídos e Tolerantes a Falhas (INF01085)

Serviço distribuído e tolerante a falhas de soma de inteiros positivos sobre UDP, desenvolvido para a disciplina **INF01085 — Sistemas Distribuídos e Tolerantes a Falhas (UFRGS)**.

O sistema organiza os servidores em um cluster com separação de papéis entre **Front-End (líder/sequenciador)** e **Replica Managers (RMs)**, utilizando descoberta por multicast (`239.192.85.2`), replicação ativa com ordenação total, quórum de maioria simples dos RMs ativos (`(n // 2) + 1`, com mínimo de 1 RM), monitoramento por heartbeats com RTT adaptativo (EWMA), eleição de líder em anel por maior IP e recuperação de estado por snapshot.

---

## Executar

Python 3.11+, sem dependências externas, em uma rede local com multicast IPv4 habilitado.

Na raiz do projeto, prepare e verifique os arquivos Python com o script automatizado:

```bash
make
make test
```

Python não exige compilação para código nativo; `make` verifica e pré-compila os módulos. Para executar com o Makefile, use `make server PORT=4000` e `make client PORT=4000`. Para o modo local, acrescente `ARGS=--local` a cada comando.

Inicie os servidores um de cada vez em máquinas distintas:

```bash
python3 servidor.py 4000
```

Aguarde o primeiro anunciar `COORDINATOR ready` (~1,5s) e cada novo RM aparecer como `RM joined` antes de iniciar o próximo. Use `--show-port` (`-p`) para incluir a porta do cliente nos logs.

Em uma ou mais máquinas clientes, execute:

```bash
python3 cliente.py 4000
```

Digite números inteiros positivos (um por linha) ou redirecione um arquivo via `<`.

### Modo local (opcional, para testes na mesma máquina)

Para simular múltiplos servidores e clientes em um único computador, adicione a flag `--local` em todos os comandos:

```bash
python3 servidor.py 4000 --local
python3 cliente.py 4000 --local
```

O modo `--local` reserva IPs sucessivos de loopback (`127.0.0.1`, `127.0.0.2`, ...) na mesma porta. No macOS, habilite antes os aliases necessários (`sudo ifconfig lo0 alias 127.0.0.2 up`, etc.).

---

## Como funciona

### Fluxo de uma requisição
1. **Descoberta:** o cliente envia `DISCOVERY` por multicast (`239.192.85.2`); apenas o **Front-End (FE)** responde.
2. **Sequenciamento:** ao receber um número (`REQUEST`), o FE atribui um número de sequência global (`seq`) e dissemina a operação (`OP`) por multicast para os **Replica Managers (RMs)**.
3. **Replicação ativa:** cada RM mantém seu próprio acumulador e aplica as operações em ordem estrita (`seq == num_reqs + 1`), devolvendo o resultado (`RESULT`) ao FE.
4. **Quórum de maioria:** o FE só confirma a operação ao cliente (`ACK`) após receber respostas concordantes da **maioria simples dos RMs ativos** (`(n // 2) + 1`, com mínimo de 1 RM).
5. **Retentativa transparente:** se o cliente não receber `ACK` em `0,5 s`, redescobre o líder e reenvia o mesmo pedido (identificado por `session` + `id_req`, evitando somas duplicadas).

### Tolerância a falhas
- **Monitoramento:** FE e RMs trocam heartbeats periódicos (`HEARTBEAT`/`ALIVE`) com timeout adaptativo baseado no RTT. Três perdas consecutivas marcam o nó como inativo.
- **Queda de um RM:** o FE remove o nó da visão ativa e ajusta o quórum automaticamente, sem interromper o serviço. Se todos os RMs caírem (`0 RMs`), novas confirmações ficam pausadas até pelo menos um RM retornar.
- **Queda do FE:** os RMs iniciam o **algoritmo do anel** (`TOKEN`/`HOP_ACK`) para eleger o nó vivo de **maior IP** como novo FE. Antes de atender clientes, o novo líder sincroniza o maior estado entre os sobreviventes.
- **Entrada ou retorno de RM:** um servidor que ingressa depois sincroniza seu acumulador e histórico via **snapshot** (`JOIN` → `SNAP` → `READY`) antes de passar a compor o quórum.

---

## Testes

### Suíte unitária e de integração (em memória)

```bash
python3 -B -m unittest discover -s tests -v
```

Executa todos os **105 testes** cobrindo protocolo, estado, descoberta UDP, cliente, heartbeat/RTT, concorrência, quórum de maioria, eleição em anel, failover e recuperação.

### Suíte E2E local (múltiplos servidores e clientes reais)

Gera automaticamente os números de teste em memória (sem depender de arquivos externos) e valida somas exatas sob falhas reais:

```bash
# Todos os 5 cenários (padrão: 3 clientes × 1.000 números cada)
python3 scripts/e2e_local.py

# Definir quantidade de números por cliente e número de clientes
python3 scripts/e2e_local.py --count 10000 --clients 3

# Cenários específicos com semente reproduzível
python3 scripts/e2e_local.py --count 5000 --clients 3 --seed 42 --scenarios normal_load fe_failover_ring
```

Cenários disponíveis: `normal_load`, `rm_crash_majority`, `fe_failover_ring`, `rm_join_under_load` e `zero_rm_block_and_recover`.

---

## Estrutura do Projeto

```text
trabalho-sdtf/
├── README.md
├── servidor.py
├── cliente.py
├── .agents/                 # Governança e histórico de sessões com IA
├── client/                  # Descoberta, envio/retransmissão e interface do cliente
├── common/                  # Protocolo JSON/UDP, configurações e utilitários
├── server/                  # Cluster, replicação ativa, quórum, heartbeat, eleição e recuperação
├── scripts/                 # Suíte de testes E2E local
├── tests/                   # Suíte unitária e de integração (unittest)
└── docs/                    # Especificações e relatórios
```

## Documentação

- [Especificação (Replicação e Eleição de Líder)](docs/INF01085%20-%20Trabalho_202602_P2.pdf)
- [Especificação (Comunicação Confiável)](docs/INF01151-Trabalho_202601_P1.pdf)
- [Documentação histórica inicial](docs/README-PARTE1.md)
- [Relatório técnico anterior](docs/Relat%C3%B3rio%20-%20INF01085%20-%20Trabalho%20Pr%C3%A1tico%201.md)
- [Governança e uso de IA](.agents/agents.md)
- [Registro de sessões com IA](.agents/ai-sessions.md)
