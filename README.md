# Sistemas Distribuídos e Tolerantes a Falhas (INF01085)

Repositório dos trabalhos práticos da disciplina **INF01085 - Sistemas Distribuídos e Tolerantes a Falhas**, ministrada pelo Prof. Weverton Cordeiro no Instituto de Informática da UFRGS.

---

## Estrutura do Projeto

O projeto é dividido em duas partes:

1. **[Parte 1: Comunicação Confiável Inter-Processos](README-PARTE1.md)**
   - Serviço distribuído cliente-servidor para soma agregada de inteiros positivos sobre UDP puro.
   - Descoberta automática de nós via broadcast na rede local.
   - Garantia de entrega *exactly-once* com retransmissões por timeout, detecção de duplicatas e gaps na camada de aplicação.
   - Estado concorrente thread-safe em memória com acumuladores de 64 bits (`uint64`).
   - Interface cliente multithreading (threads desacopladas para leitura de `stdin` e escrita no terminal).
   - [Documentação Detalhada e Guia de Execução da Parte 1](README-PARTE1.md)

2. **Parte 2: Replicação e Tolerância a Falhas**
   - Escopo: replicação de máquinas de estados e persistência distribuída consistente (a ser desenvolvida).

---

## Guia Rápido de Execução (Quick Start)

### Pré-requisitos
- Python 3.11+
- Nenhuma dependência externa necessária (utiliza exclusivamente a biblioteca padrão do Python).

### 1. Iniciar o Servidor
Em um terminal, escolha uma porta (ex: `4000`):
```bash
python3 servidor.py 4000
```
*(Opcional: use `python3 servidor.py 4000 --show-port` para exibir `ip:porta` nos logs).*

### 2. Iniciar o Cliente
Em outro terminal (na mesma máquina ou em outro computador na mesma rede local):
```bash
python3 cliente.py 4000
```
O cliente descobrirá o IP do servidor automaticamente via broadcast. A partir daí, basta digitar números inteiros e teclar `ENTER`.

### 3. Teste em Lote com Arquivo de Dados
```bash
python3 cliente.py 4000 < test_data/test_100.txt
```

### 4. Executar Testes Automatizados
```bash
# Testes unitários e de integração
python3 -m unittest discover tests
```

---

## Documentos e Recursos

- **Especificação Oficial:** [INF01151-Trabalho_202601_P1.pdf](docs/INF01151-Trabalho_202601_P1.pdf)
- **Documentação Técnica da Parte 1:** [README-PARTE1.md](README-PARTE1.md)
- **Gabarito de Testes de Carga:** [test_data/README.md](test_data/README.md)