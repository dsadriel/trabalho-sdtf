# Sistemas Distribuídos e Tolerantes a Falhas (INF01085)

Serviço de soma de inteiros positivos sobre UDP, desenvolvido para a disciplina INF01085 da UFRGS.

A Parte 2 adapta a implementação da Parte 1: mantém `Message`, `ServerState`, `ServerInterface`, `handle_discovery`, `handle_request` e a interface do cliente. Acrescenta descoberta do coordenador por multicast, replicação ordenada com quórum de maioria simples dos RMs ativos (`(n // 2) + 1`, mínimo 1 RM), monitoramento de falhas, eleição em anel e recuperação por snapshot.

## Executar

Python 3.11+, sem dependências externas. Para a demonstração em Linux, inicie os servidores em terminais separados:

```bash
python3 servidor.py 4000 --local
```

Espere o primeiro anunciar `COORDINATOR ready`; inicie os próximos um de cada vez e espere a entrada de cada réplica no grupo. O modo `--local` reserva endereços de loopback diferentes na mesma porta. `--show-port` ou `-p` inclui a porta do cliente nos logs de pedidos.

Em outro terminal:

```bash
python3 cliente.py 4000 --local
```

Digite números positivos, um por linha. Para máquinas distintas, omita `--local` e use uma rede com multicast IPv4 habilitado. A descoberta usa o grupo `239.192.85.2`; somente o líder responde. Veja os passos de demonstração e as limitações no [LEIA-ME.md](LEIA-ME.md).

## Fluxo do código

- `cliente.py` chama `discover_server` e `run_client_interface`. A interface mantém `user_input_reader` e `screen_writer`.
- `send_request_and_wait_ack` retransmite o mesmo pedido e redescobre o líder após timeout, preservando sessão e identificador do pedido.
- `servidor.py` cria `Node`, inicia `ServerInterface` e executa o laço UDP.
- `Node.handle` chama `handle_discovery`, `handle_request` e `handle_exit`. Os demais tipos são encaminhados aos tratadores `handle_*` de eleição, heartbeat, recuperação e replicação.
- O FE mantém várias operações pendentes, com registro de resultados e participantes por época/sequência. Retransmissões reutilizam o trabalho existente.
- `handle_request` aguarda a confirmação da maioria simples dos RMs participantes (`(n // 2) + 1`, exigindo pelo menos 1 RM ativo) antes de chamar `ServerState.process_request` e responder. Um RM só é retirado da visão ativa após três perdas consecutivas de heartbeat; se não restar nenhum RM, o FE cancela operações pendentes e aguarda o ingresso de pelo menos uma réplica. As réplicas processam em threads e aplicam operações em ordem usando esse mesmo método. A espera pela rede não mantém um lock global entre pedidos.
- `ServerState` concentra soma, contador e histórico de clientes; seus snapshots carregam também os dados necessários para detectar duplicatas após eleição.
- Após uma eleição, o FE coleta e sincroniza o estado dos sobreviventes antes de atender. Geração de liderança e confirmações de snapshot evitam que réplicas continuem presas ao coordenador antigo.
- Um pedido que revele histórico incompleto recebe `HISTORY_GAP`; o cliente informa o erro e interrompe o envio, sem redescoberta infinita.
- A janela configurável `MAX_INFLIGHT_OPERATIONS` limita os trabalhos em andamento. Os RMs conservam respostas por sequência até o FE confirmar o prefixo, permitindo retransmissões após operações posteriores.
- `Message` e `MessageType` representam tanto as mensagens da aplicação quanto as do cluster. Há um único caminho de codificação e decodificação.

## Testes

```bash
python3 -B -m unittest discover -s tests -v
python3 -B -m unittest test_cluster -v
```

A primeira suíte cobre protocolo, estado, descoberta UDP local, cliente, heartbeat, concorrência, retransmissões e regressões de troca de líder com quatro nós, perdas de mensagens e quedas sucessivas. A segunda usa o despacho e os tratadores reais com transporte em memória para testar replicação, quórum, eleição, duplicatas, reordenação e recuperação. A comunicação multicast entre processos deve ser validada no ambiente Linux de execução.

## Documentação

- [Guia da Parte 2](LEIA-ME.md)
- [Documentação histórica da Parte 1](README-PARTE1.md)
- [Especificação da Parte 1](docs/INF01151-Trabalho_202601_P1.pdf)
- [Especificação da Parte 2](docs/INF01085%20-%20Trabalho_202602_P2.pdf)
- [Registro de interações com IA](ai-sessions.md)
