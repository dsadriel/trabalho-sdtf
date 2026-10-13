#!/bin/bash

ARQUIVO="test_data/RAND_NUM_1.txt"
PORTA=4000
NUM_CLIENTES=3

# Função para limpar os processos e arquivos caso aperte Ctrl + C
cleanup() {
    echo -e "\n[!] Ctrl + C detectado! Encerrando todos os clientes..."
    # Mata todos os processos em background iniciados por este script
    kill $(jobs -p) 2>/dev/null
    # Remove a pasta temporária
    rm -rf temp_parts
    exit 1
}

# Captura o sinal do Ctrl + C (SIGINT) e chama a função cleanup
trap cleanup SIGINT

if [ ! -f "$ARQUIVO" ]; then
    echo "Erro: Arquivo $ARQUIVO não encontrado!"
    exit 1
fi

echo "Dividindo o arquivo em $NUM_CLIENTES partes..."
mkdir -p temp_parts
split -n l/$NUM_CLIENTES "$ARQUIVO" temp_parts/parte_

echo "Iniciando os clientes..."

CORES=('\033[0;32m' '\033[0;36m' '\033[0;35m')
RESET='\033[0m'

i=0
for pedaco in temp_parts/parte_*; do
    COR=${CORES[$i]}
    NUM_CLIENTE=$((i + 1))
    
    echo -e "${COR}Iniciando Cliente $NUM_CLIENTE${RESET} processando $(basename "$pedaco")"
    
    python3 -u cliente.py $PORTA < "$pedaco" 2>&1 | sed -e "s/^/[Cliente $NUM_CLIENTE] /" &
    
    i=$((i + 1))
done

wait
echo -e "\nTodos os clientes finalizaram o envio!"

rm -rf temp_parts