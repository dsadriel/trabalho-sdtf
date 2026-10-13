# Parte 1: Comunicação Confiável Inter-Processos

Serviço distribuído cliente-servidor de soma de inteiros sobre UDP puro, com descoberta dinâmica por broadcast e entrega confiável *exactly-once* implementada na camada de aplicação.

- **Especificação:** [INF01151-Trabalho_202601_P1.pdf](docs/INF01151-Trabalho_202601_P1.pdf)

---

## Como Executar

### 1. Iniciar o Servidor
No terminal da máquina servidora:
```bash
python3 servidor.py <porta>
```
*Exemplo:*
```bash
python3 servidor.py 4000
```
> **Dica:** Passe `--show-port` (ou `-p`) para inspecionar a porta efêmera dos clientes nos logs: `python3 servidor.py 4000 --show-port`.

### 2. Iniciar o Cliente
No terminal da máquina cliente (ou outro terminal local):
```bash
python3 cliente.py <porta>
```
*Exemplo:*
```bash
python3 cliente.py 4000
```
O cliente realiza broadcast na porta informada, localiza o servidor e imprime:
```text
2026-10-04 20:00:00 server_addr 192.168.15.7
```
Em seguida, digite números inteiros e pressione `ENTER` para somar. Para sair, tecle `Ctrl+C` ou `Ctrl+D`.

### 3. Execução em Lote com Arquivos de Teste
Você pode alimentar o cliente diretamente via redirecionamento de entrada (`<`):
```bash
# Execução com visualização de respostas
python3 cliente.py 4000 < test_data/test_100.txt

# Execução em alta velocidade (silenciando saída do cliente no terminal)
python3 cliente.py 4000 < test_data/test_1000.txt > /dev/null
```

---

## Protocolo de Comunicação (JSON sobre UDP)

As mensagens são serializadas em JSON codificado em UTF-8:

| Mensagem (`type`) | Direção | Campos | Descrição |
| :--- | :--- | :--- | :--- |
| `DISCOVERY` | Cliente $\rightarrow$ Servidor (Broadcast) | — | Procura o servidor na rede local |
| `DISCOVERY_RESP` | Servidor $\rightarrow$ Cliente (Unicast) | — | Resposta de confirmação da descoberta |
| `REQUEST` | Cliente $\rightarrow$ Servidor (Unicast) | `id_req` (uint64), `value` (int) | Envio de número para soma |
| `ACK` | Servidor $\rightarrow$ Cliente (Unicast) | `id_req` (uint64), `num_reqs` (uint64), `total_sum` (uint64) | Confirmação com soma acumulada |
| `EXIT` | Cliente $\rightarrow$ Servidor (Unicast) | — | Aviso de encerramento da sessão do cliente |

---

## Formato Oficial das Mensagens no Terminal

O sistema atende estritamente à formatação exigida pela especificação:

- **Inicialização do Servidor:**
  `<data> num_reqs 0 total_sum 0`
- **Descoberta no Cliente:**
  `<data> server_addr <ip_servidor>`
- **Requisição no Servidor (Sucesso):**
  `<data> client <ip_cliente> id_req <id> value <v> num_reqs <n> total_sum <s>`
- **Requisição Duplicada no Servidor:**
  `<data> client <ip_cliente> DUP!! id_req <id> value <v> num_reqs <n> total_sum <s>`
- **Confirmação no Cliente (ACK):**
  `<data> server <ip_servidor> id_req <id> value <v> num_reqs <n> total_sum <s>`

---

## Organização do Código

O projeto segue modularização estrita por papel e subserviço:

```text
trabalho-sdtf/
├── servidor.py              # Ponto de entrada do servidor
├── cliente.py               # Ponto de entrada do cliente
├── server/
│   ├── discovery.py         # Descoberta passiva (responde broadcast e registra cliente)
│   ├── processing.py        # Processamento, validação de id_req, detecção de gaps e duplicatas
│   └── state.py             # Estado thread-safe em memória (tabela de clientes e acumulador global)
├── client/
│   ├── discovery.py         # Descoberta ativa (envia broadcast e aguarda resposta)
│   ├── processing.py        # Processamento (envia requisições e retransmite com timeout)
│   └── interface.py         # Interface multithread (thread leitora de stdin e escritora na tela)
├── common/
│   ├── protocol.py          # Definição unificada da classe Message e serialização JSON
│   ├── config.py            # Constantes de rede (buffer, timeouts, limites de portas)
│   └── utils.py             # Utilitários compartilhados (timestamp formatado e parser de porta)
├── test_data/               # Datasets de teste incrementais (10 a 100.000 números) e gabaritos
└── tests/                   # Suíte de testes unitários e de integração
```

### Regra de Dependência
- `server/` e `client/` **nunca** importam um ao outro diretamente.
- O código compartilhado reside exclusivamente em `common/`.
- O estado global pertence exclusivamente a `server/state.py`.

---

## Testes Automatizados

Para executar todos os testes unitários e de integração:
```bash
python3 -m unittest discover tests
```
