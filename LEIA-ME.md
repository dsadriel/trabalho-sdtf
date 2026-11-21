# Serviço de soma — Parte 2

Python 3.11 ou superior em Linux. No mesmo hospedeiro, abra três terminais e execute em cada um:

    python3 servidor.py 4000 --local

Espere o primeiro anunciar COORDINATOR ready e cada novo RM aparecer no log antes de iniciar o próximo processo. O modo local tenta reservar IPs sucessivos de loopback pela mesma porta. Em máquinas distintas, omita --local em todos os comandos e use uma rede com multicast IPv4 habilitado. O servidor identifica seu IP pela rota de saída.

Em dois outros terminais execute:

    python3 cliente.py 4000 --local

Digite 10 num cliente e 20 no outro. Confira o total 30. Encerre abruptamente o FE e observe os logs de eleição nos RMs. Envie outro valor e confira a soma. Encerre um RM com o FE ativo e observe o ajuste de membros sem eleição. Inicie outro servidor para acompanhar a sincronização.

O FE exige confirmação de todos os RMs participantes antes de enviar ACK ao cliente (quórum de unanimidade). Sem RMs, o FE processa localmente, sem redundância. A demonstração pressupõe inicialização escalonada e quedas de processo numa rede local conectada. Partições de rede não são toleradas. O estado de recuperação deve caber num datagrama UDP. Não há persistência em disco.

O cliente espera pelo ACK correto até um prazo absoluto de `TIMEOUT_SECS` (0,5 s). Um ACK válido encerra a espera imediatamente. ACKs antigos, respostas de descoberta atrasadas, pacotes de outro remetente ou mensagens inválidas são ignorados sem reiniciar o prazo nem reenviar o pedido. Somente a expiração da espera ou um erro UDP de conexão provoca redescoberta e reenvio. A descoberta também aguarda a resposta válida até seu prazo, sem reenviar DISCOVERY por causa de cada pacote alheio. Duplicatas continuam possíveis quando uma resposta realmente se perde, sendo tratadas pelo histórico do servidor.

    python3 -B -m unittest test_cluster

O teste usa os tratadores reais com transporte em memória. O fluxo de rede deve ser validado em Linux.

No PowerShell, use `python servidor.py 4000 --local` e `python cliente.py 4000 --local` para testes no mesmo computador. O transporte usa bind em endereço local curinga no Windows e inscrição no grupo por `IP_ADD_MEMBERSHIP`. Foram verificados descoberta, replicação e eleição com três nós usando sockets reais no Windows, em um único processo de teste. A demonstração com processos separados e a validação no Linux continuam necessárias.

## RTT, heartbeats e detecção de falhas

Cada servidor monitora os seus pares: o FE monitora as réplicas da visão atual, e cada réplica monitora o FE. Envia `HEARTBEAT` e espera `ALIVE` com o mesmo identificador de sondagem e a mesma época. O RTT é medido com relógio monotônico local, sem depender de relógios sincronizados entre máquinas.

O timeout individual é `max(RTT_MIN_TIMEOUT, 3 × RTT médio)`. A primeira amostra inicializa a média; as seguintes usam média móvel exponencial, com peso `RTT_ALPHA` para a nova amostra. Antes de qualquer amostra, vale `RTT_INITIAL_TIMEOUT = 0.1` segundo. O piso padrão é 50 ms tanto no Linux quanto no Windows (`RTT_MIN_TIMEOUT = 0.05`), ajustável em `common/config.py`. Esse valor é um parâmetro de proteção ao scheduler escolhido para o experimento, não uma medição automática do ambiente. Não há teto que limite uma rede mais lenta a prazos do laboratório; ajuste também o timeout inicial se o ambiente exigir mais tempo para obter a primeira amostra.

Os logs de perda incluem `timeout_ms` aplicado à sondagem e `rtt_ms` médio, para diagnóstico. Uma resposta válida zera as perdas consecutivas. Apenas três sondagens distintas expiradas declaram o nó inativo. Respostas de sondagens antigas, de outra época ou recebidas após o prazo não zeram a contagem. Uma pausa longa no processo conta como uma única perda da sondagem pendente, sem inventar três perdas de mensagens que não foram enviadas.

Depois de uma resposta válida, o próximo heartbeat é agendado após o timeout estimado daquele par; após uma perda, a próxima sondagem começa imediatamente. Com RTT de 1 ms e piso de 50 ms, três perdas exigem aproximadamente 150 ms desde o envio da primeira sondagem perdida, mais os atrasos de execução; se a falha ocorrer entre sondagens, há também o intervalo até o próximo envio. Isso não é uma garantia de tempo real. O `select` espera até o próximo vencimento pendente (heartbeat, ACK da eleição, descoberta inicial ou anúncio), retornando antes se chegar uma mensagem. Não há polling fixo de 100 ms.

Os ACKs de passagem do token de eleição também usam `3 × RTT médio` do respectivo par, com o mesmo valor inicial e piso. A espera pela rodada de eleição considera o tamanho do anel e os timeouts estimados. O `VIEW` anuncia os membros a cada 0,3 s na operação normal; durante a sincronização, o anúncio é repetido também por unicast aos participantes pendentes, em intervalos guiados pelos timeouts desses pares. O timeout do cliente permanece em 0,5 s e a confirmação de operações exige todos os RMs participantes; a descoberta inicial de servidores mantém sua janela de 1,5 s.

## Troca de líder e recuperação

A eleição continua sendo em anel, com precedência por IP. Um nó que recebe um token válido passa a participar da eleição mesmo que ainda não tenha detectado a queda do FE. Os anúncios incluem uma geração inteira de liderança (`term`) e a época única (`epoch`). Entre anúncios da mesma geração, o maior IP tem precedência. Mensagens de liderança anteriores não desfazem uma transição já reconhecida. Isso permite convergência das rodadas concorrentes numa rede local conectada; não constitui tolerância a partições.

Após vencer, o FE entra em `synchronizing`. Preserva o maior estado entre o snapshot do token e sua memória local, coleta os snapshots dos participantes ainda vivos por `JOIN` e seleciona o de maior sequência global. Depois distribui `SNAP` e aguarda `READY` com a sequência escolhida. Anúncios e solicitações são repetidos; um participante só é removido após as três perdas de heartbeat. Apenas após essa etapa aparece `COORDINATOR ready`, liberando descoberta e pedidos de clientes. Os logs mostram geração, sequência e soma para conferir a recuperação.

Uma réplica não substitui seu estado por um snapshot de sequência menor: informa seu estado ao coordenador. O anúncio periódico carrega a sequência atual do FE; réplicas atrasadas solicitam sincronização mesmo sem novas operações chegando. Assim, a perda da última operação também pode ser reparada.

Quando o FE recebe um pedido acima do próximo identificador esperado da sessão, registra `HISTORY GAP` e envia `HISTORY_GAP` com os identificadores recebido/conhecido e o contador global. O cliente informa `Histórico incompleto`, interrompe o envio e termina com código 1. Não pula pedidos nem transforma esse erro em descobertas repetidas. ACKs antigos e pacotes alheios continuam sendo ignorados dentro do prazo normal.

Cada pedido captura os RMs participantes. O FE só confirma ao cliente depois que todos os participantes ainda ativos confirmarem a mesma sequência e soma. RESULT perdido ou RM lento não reduz a exigência: a operação é reenviada e o FE continua esperando enquanto o RM responde aos heartbeats. Uma resposta duplicada, de outro nó ou de outra época não substitui a confirmação pendente.

Somente o detector de falhas pode retirar um RM dessa espera, depois de três sondagens consecutivas sem resposta. A remoção acorda a thread do pedido, que reavalia as confirmações exigidas sem esperar o processo retornar. Isso é uma suspeita operacional baseada em timeout: UDP e atrasos não permitem provar uma queda física. A demonstração pressupõe rede conectada, sem partições, e parâmetros adequados ao scheduler. Se nenhum RM restar, o FE processa localmente, sem redundância externa.

Um RM novo não entra no conjunto das operações já em andamento: sua chegada pausa novas admissões, o pipeline existente é concluído e ele recebe o snapshot confirmado antes de ser admitido. Se o RM cair durante essa entrada, três perdas de heartbeat removem também essa espera de ingresso. Os pedidos seguintes exigem também a confirmação desse RM. No log, `quorum 3` com três RMs significa três confirmações obrigatórias.

Com essa política, uma operação confirmada já foi aplicada por todos os RMs que permaneceram participantes. Um sobrevivente que participou dessa confirmação conserva a operação para a próxima eleição. Um processo recém-iniciado ou excluído por suspeita ainda precisa sincronizar; não há recuperação dos dados se todas as cópias forem encerradas. A mensagem de histórico incompleto detecta lacunas daquela sessão e não substitui persistência em disco.

Para repetir as regressões com transporte em memória:

    python3 -B -m unittest discover -s tests -p test_all_replicas.py -v


    python3 -B -m unittest discover -s tests -p test_failover.py -v

Para a validação manual, reinicie todos os servidores e clientes na mesma versão. Abra quatro servidores locais, mantenha o cliente enviando um arquivo e encerre o FE. Confira o anúncio `COORDINATOR ready` no vencedor e o reconhecimento desse coordenador nos demais. Continue os pedidos, encerre simultaneamente os dois RMs restantes e confira a continuidade da soma no único sobrevivente, agora com quórum zero. A execução com processos reais e rede do laboratório continua necessária.

## Concorrência no FE e nos RMs

O FE cria uma thread por pedido distinto admitido. Retransmissões do mesmo `(epoch, sessão, id_req)` se associam ao trabalho existente, sem reservar outra sequência. Cada `PendingOperation` guarda sequência, valor, soma esperada, participantes e confirmações independentes. O contador de sequências reservadas é separado do prefixo confirmado em `ServerState`; portanto vários pedidos podem estar em trânsito enquanto uma operação anterior ainda espera confirmação.

O lock global que cobria a espera pela rede foi removido. A condição do nó protege as transições de metadados e atualizações do estado; `wait` libera esse lock enquanto uma thread aguarda resultados. Cada operação só altera o estado confirmado e envia seu ACK quando todas as suas réplicas ainda ativas confirmaram e ela é a próxima sequência do prefixo. Uma confirmação da sequência 3 não confirma as sequências 1 ou 2. Pedidos da mesma sessão preservam sua ordem, enquanto sessões diferentes avançam concorrentemente.

Nos RMs, `handle_op` admite uma thread por operação distinta. Uma operação fora de ordem espera pela sequência anterior, liberando a condição durante a espera. A aplicação do acumulador, contador e histórico é atômica e ordenada. O recebimento de novas mensagens e heartbeats continua no laço de transporte. Duplicatas em andamento não criam threads adicionais.

Cada RM mantém um cache temporário de RESULT por sequência. Se uma resposta antiga se perder, ela pode ser reenviada mesmo após o RM aplicar operações posteriores. O FE divulga o prefixo confirmado em OP/VIEW; somente os resultados até esse prefixo podem ser descartados. Isso evita que a retransmissão de uma operação antiga fique sem resposta e permite manter memória proporcional à janela em andamento.

Trocas de liderança e instalação de snapshots cancelam os trabalhos antigos e acordam as threads. Workers de RM verificam a época e uma versão local da instalação de estado antes de aplicar dados, impedindo que uma thread antiga modifique um snapshot novo. A recuperação usa o prefixo aplicado disponível nos sobreviventes; pedidos sem ACK podem ser retransmitidos na nova época, conservando sessão e id_req.

`MAX_INFLIGHT_OPERATIONS = 64`, em `common/config.py`, limita a janela e os workers por papel. É um parâmetro de recursos ajustável por medição, não uma condição de corretude. Ao atingir o limite de admissão, novos datagramas podem ser ignorados e o cliente usa o reenvio existente; isso evita criar um número ilimitado de threads. `OP_RETRY_INTERVAL = 0.2` s mantém o intervalo de retransmissão de operações separado dos timeouts de heartbeat/eleição por RTT. Um único cliente continua com um pedido por vez; a janela é aproveitada principalmente por múltiplos clientes.

Testes específicos, com transporte em memória e threads reais:

    python3 -B -m unittest discover -s tests -p test_operations.py -v
    python3 -B -m unittest discover -s tests -p test_concurrency.py -v
    python3 -B -m unittest discover -s tests -p test_replica_workers.py -v

A validação desses cenários não é um benchmark de rede nem comprova uma taxa para 10 milhões de pedidos. Medições no Linux do laboratório continuam necessárias.

## Organização da Parte 2

- `cliente.py` e `servidor.py` inicializam os processos.
- `client/discovery.py` mantém `discover_server`, agora por multicast. `client/processing.py` mantém `send_request_and_wait_ack`, acrescentando redescoberta do líder sem mudar a identidade do pedido.
- `client/interface.py` mantém `run_client_interface`, `user_input_reader` e `screen_writer` da Parte 1.
- `server/cluster.py` guarda a visão do cluster e despacha mensagens para `handle_discovery`, `handle_request`, `handle_exit` e os novos `handle_*`. `server/transport.py` gerencia os sockets UDP.
- `server/state.py` mantém `ServerState` e `ClientRecord` como estado único do serviço. Líder e réplicas usam `process_request`; `snapshot` e `restore` preservam o histórico por sessão.
- `common/protocol.py` estende `Message` e `MessageType` com os tipos do cluster. `server/interface.py` reutiliza `ServerInterface` para logs e saída assíncrona.
- `server/processing.py` sequencia no FE, `server/replication.py` aplica nos RMs e `server/quorum.py` conta os resultados concordantes.
- `server/heartbeat.py` acompanha a disponibilidade, `server/election.py` executa o anel e `server/recovery.py` sincroniza as réplicas.

Execute também os testes de estado, protocolo, descoberta e cliente:

    python3 -B -m unittest discover -s tests -v
