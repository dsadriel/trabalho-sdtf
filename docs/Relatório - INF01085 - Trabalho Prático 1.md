# **Relatório \- Trabalho Prático Parte 1: Comunicação Confiável Inter-Processos**

## **Universidade Federal do Rio Grande do Sul (UFRGS)** **INF01085 \- Sistemas Distribuídos E Tolerantes A Falhas \- Turma U** 

**Professor:** Weverton Cordeiro  
**Semestre:** 2026/2

# **Equipe**

Adriel Souza \| 00579100  
Gabriel Richter \| 00587414  
Nicolas Chin Lee \| 00579322  
Vicente Pianezzola \| 00587456

**1\. IMPLEMENTAÇÃO DOS SUBSERVIÇOS**

**1.1 Arquitetura geral**

O projeto foi dividido em módulos com separação estrita de responsabilidades:

* cliente.py / servidor.py: Pontos de entrada dos programas  
* client/ e server/: Subserviços de descoberta, processamento e interface  
* server/state.py: Estado compartilhado e thread-safe do servidor  
* common/:  Protocolo, configurações e utilitários compartilhados

A comunicação entre os processos usa mensagens JSON codificadas em UTF-8 transmitidas via sockets UDP.

**1.2 Subserviço de Descoberta**

O cliente inicia chamando discover\_server() em client/discovery.py. A função habilita SO\_BROADCAST, envia uma mensagem DISCOVERY para 255.255.255.255 e aguarda até 1 segundo por DISCOVERY\_RESP. Se não receber resposta, retransmite indefinidamente até o servidor responder.

O servidor recebe a mensagem no loop principal de servidor.py e chama handle\_discovery() em server/discovery.py. A função registra o cliente em ServerState usando o par (IP, porta efêmera) como chave e responde com DISCOVERY\_RESP por unicast. Esse par identifica cada processo cliente de forma única, inclusive quando vários executam na mesma máquina.

**1.3 Subserviço de Processamento**

O cliente chama send\_request\_and\_wait\_ack() em client/processing.py, que envia uma mensagem REQUEST com o id\_req atual e o valor lido do stdin. Aguarda o ACK correspondente com timeout de 20 ms. Em caso de timeout, retransmite a mesma mensagem sem incrementar o id\_req. O cliente nunca envia a próxima requisição antes de confirmar a atual (stop-and-wait).

No servidor, handle\_request() em server/processing.py recebe a mensagem e chama state.process\_request(), que retorna um dos três estados possíveis:

\- NORMAL (id\_req \== last\_req \+ 1): soma aceita, contadores atualizados.  
\- DUPLICATE (id\_req \<= last\_req): valor ignorado, ACK do estado anterior reenviado.  
\- GAP (id\_req \> last\_req \+ 1): valor ignorado, ACK com o último id\_req confirmado.

Em todos os casos, o ACK é enviado antes do log, evitando que latência de I/O atrase a resposta ao cliente.

**1.4 Subserviço de Interface**

No cliente, run\_client\_interface() em client/interface.py cria duas threads:  
\- user\_input\_reader: lê stdin, valida entradas e envia requisições.  
\- screen\_writer: consome uma fila e imprime confirmações no terminal.

Ao encerrar (EOF ou Ctrl+C), a thread principal envia EXIT ao servidor e coloca o sentinela None na fila para que a thread escritora imprima as mensagens pendentes antes de terminar.

No servidor, a classe ServerInterface em server/interface.py gerencia uma thread dedicada à escrita de logs. O loop principal enfileira mensagens em O(1) e volta imediatamente ao recvfrom(). A thread consumidora faz o print() de forma assíncrona, sem bloquear o processamento de novos datagramas.

**2\. SINCRONIZAÇÃO NO ACESSO A DADOS**

**2.1 ServerState**

A classe ServerState em server/state.py usa um threading.Lock (\_lock) para proteger todo acesso à tabela de clientes e aos contadores globais (num\_reqs, total\_sum).

O método process\_request() executa atomicamente dentro do lock:  
  1\. Verificação do id\_req recebido versus last\_req do cliente.  
  2\. Detecção de DUPLICATE ou GAP.  
  3\. Atualização de num\_reqs e total\_sum.  
  4\. Atualização dos campos last\_\* do ClientRecord.

Manter essas etapas na mesma seção crítica garante que dois ACKs nunca carreguem o mesmo valor de num\_reqs e que a soma nunca seja atualizada parcialmente.

**2.2 Filas de log**

Ambas as filas (cliente e servidor) usam queue.Queue, que é internamente thread-safe via threading.Condition, dispensando locks manuais nos pontos de put() e get().

**3\. PRINCIPAIS ESTRUTURAS E FUNÇÕES**

**3.1 Message e MessageType (common/protocol.py)**

MessageType é um StrEnum com os cinco tipos do protocolo: DISCOVERY, DISCOVERY\_RESP, REQUEST, ACK e EXIT. O uso de StrEnum permite que o valor serializado em JSON seja diretamente a string do enum.

Message representa qualquer mensagem do protocolo. Construtores nomeados (classmethods) garantem combinações válidas de campos. encode() serializa para JSON UTF-8; decode() desserializa e valida, lançando ValueError para payloads malformados, que são descartados pelo loop principal.

**3.2 ClientRecord e ServerState (server/state.py)**

ClientRecord é um dataclass com os campos ip, port, last\_req, last\_num\_reqs e last\_total\_sum. Os campos last\_num\_reqs e last\_total\_sum permitem reenviar o ACK idêntico ao original em caso de duplicata, sem reconsultar os contadores globais que já avançaram com outros clientes.

ServerState mantém a tabela de clientes (dict keyed por (ip, port)), num\_reqs e total\_sum globais, e um threading.Lock. O método process\_request() é a função central do serviço.

**3.3 ServerInterface (server/interface.py)**

Implementa o padrão Produtor-Consumidor. A fila \_queue: Queue\[str \| None\] desacopla o loop de rede do console. Os métodos start() e stop() controlam o ciclo de vida da thread consumidora. A instância é criada em servidor.py e passada explicitamente para handle\_request() como dependência, evitando estado global.

**4\. PRIMITIVAS DE COMUNICAÇÃO INTER-PROCESSOS**

**4.1 Sockets UDP (socket.SOCK\_DGRAM)**

Toda a comunicação usa a API de sockets UDP sobre IPv4. Não há conexão ou garantia de entrega na camada de transporte — toda confiabilidade é implementada na aplicação.

\- bind("", port) no servidor: escuta em todas as interfaces (INADDR\_ANY).  
\- setsockopt(SO\_BROADCAST): habilita envio para 255.255.255.255 no cliente.  
\- settimeout(TIMEOUT\_SECS): dispara socket.timeout para retransmissão.  
\- sendto(data, addr): unicast para cliente ou servidor; broadcast na descoberta.  
\- recvfrom(BUFFER\_SIZE): bloqueante, retorna bytes e endereço de origem.

**4.2 threading.Thread**

Duas threads são criadas explicitamente:  
\- screen\_writer no cliente: isola o I/O de console da thread de rede.  
\- ServerInterface.\_worker no servidor: isola o I/O de console do loop UDP.

**4.3 threading.Lock**

\- ServerState.\_lock: protege a tabela de clientes e os contadores globais.  
\- ServerInterface.\_lock: protege start() e stop() contra chamadas concorrentes.

**4.4 queue.Queue**

Escolhida como estrutura de comunicação inter-thread pelos seguintes motivos:  
\- FIFO estrito: mantém a ordem cronológica dos logs.  
\- Thread-safety nativa: dispensando locks manuais nos sites de put() e get().  
\- O(1) em ambas as extremidades, baseada em collections.deque.  
\- Bloqueio eficiente: get() suspende a thread consumidora sem busy-waiting, usando Condition Variable do sistema operacional.

**5\. PROBLEMAS ENCONTRADOS E SOLUÇÕES**

**5.1 Timeouts excessivos com múltiplos clientes concorrentes**

Com 3 clientes simultâneos, o número de timeouts e retransmissões era significativo. A causa era o print() síncrono no loop principal do servidor: ao redirecionar a saída para um arquivo ou pipe, o Python ativa buffering de bloco e o print() pode bloquear por vários milissegundos a depender do sistema operacional, podendo assim causar timeout e forçando o cliente a re-enviar uma requisição que viria a ser recebida.

Solução: o sendto(ACK) foi movido para antes do print(), e a escrita de logs foi delegada à thread dedicada ServerInterface. O loop retorna ao recvfrom() em microssegundos, sem nenhuma latência de I/O. O timeout também foi ajustado para 500 ms. Nos testes com 1.000.000 de requisições divididas entre 3 clientes, localizados em outra máquina da rede, o sistema processou todas as requisições com poucas duplicações (timeouts), resultando sempre na soma acumulada correta.

**5.2 Estado global de logs com a palavra-chave \`global\`**

A primeira implementação do servidor de logs usava variáveis de módulo e a palavra-chave global para modificá-las dentro das funções de ciclo de vida. Esse padrão é frágil e dificulta testes isolados.

Solução: a lógica foi encapsulada na classe ServerInterface. Thread, fila e lock passaram a ser atributos de instância, eliminando o uso de global. A instância única é criada em servidor.py e passada como dependência explícita para handle\_request().

**5.3 Ajuste de timeout e mitigação de retransmissões em hardware limitado**

Durante os testes de estresse, observou-se que configurar um *timeout* muito baixo nos clientes gerava uma sobrecarga massiva de requisições duplicadas (DUP\!\!) no servidor. Percebeu-se que, quanto maior o tempo de timeout configurado, menor era a taxa de retransmissões indevidas. Assim, com o aumento da tolerância dos clientes pela resposta do servidor, de 0.02s para 0.5s notou-se a eliminação desse excesso de duplicatas e aumento da velocidade de processamento global, permitindo que o servidor dedicasse seus recursos a processar novas requisições em vez de descartar pacotes repetidos.

Esse comportamento justifica-se pelo ambiente físico de testes. O servidor foi executado em um hardware antigo, equipado apenas com um processador Intel Core i5-3210M e 8 GB de RAM. Sob concorrência extrema, o acúmulo de mensagens na fila de rede do sistema operacional gerava atrasos naturais que frequentemente ultrapassam *timeouts* mais curtos. Aumentar a tolerância de espera dos clientes alinhou o protocolo à capacidade real de processamento desse hardware, estabilizando a comunicação e otimizando o fluxo.

