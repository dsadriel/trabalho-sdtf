# Registro de Interações com Ferramentas de Inteligência Artificial (agents.md)

Este documento estabelece as diretrizes de governança, integridade acadêmica e divisão de responsabilidades da equipe quanto ao uso de ferramentas de Inteligência Artificial generativa no projeto prático da disciplina **INF01085 - Sistemas Distribuídos e Tolerantes a Falhas (UFRGS)**, atendendo à especificação da Parte 2 (`docs/INF01085 - Trabalho_202602_P2.pdf`).

O histórico cronológico de cada sessão de trabalho interativa com o assistente está registrado no arquivo complementar **[ai-sessions.md](ai-sessions.md)**.

---

## 1. Diretrizes e Compromisso de Integridade Acadêmica

1. **Suporte, Não Substituição:** O assistente de IA é utilizado exclusivamente como suporte ao processo de estruturação, documentação, elaboração de testes, automação e depuração. Ele **não substitui** a compreensão conceitual dos modelos de sistemas distribuídos nem o raciocínio técnico da equipe.
2. **Revisão e Validação Humana:** Qualquer código, script, diagrama ou texto gerado ou sugerido por IA passa por revisão crítica, validação de corretude conceitual e teste empírico pelos integrantes do grupo antes de ser incorporado à base de código.
3. **Auditoria Transparente:** Todas as solicitações (*prompts*), propósitos específicos, módulos afetados e resultados obtidos são catalogados cronologicamente no arquivo de sessões.

---

## 2. Matriz de Atribuições e Divisão de Responsabilidades da Equipe

Conforme exigido na especificação (item 3 da seção do relatório técnico da Parte 2), esta seção formaliza os responsáveis por cada componente do trabalho prático:

| Integrante | Matrícula | Concepção Arquitetural | Implementação | Testes & Validação | Documentação & Relatório |
| :--- | :---: | :---: | :---: | :---: | :---: |
| *Integrante 1* | — | [ ] | [ ] | [ ] | [ ] |
| *Integrante 2* | — | [ ] | [ ] | [ ] | [ ] |
| *Integrante 3* | — | [ ] | [ ] | [ ] | [ ] |
| *Integrante 4* | — | [ ] | [ ] | [ ] | [ ] |

*(Os nomes e matrículas devem ser preenchidos pelos membros do grupo de 4 integrantes antes da submissão final).*

---

## 3. Matriz de Rastreabilidade do Uso de IA por Área do Projeto

| Área do Projeto | Nível de Emprego | Propósito Declarado | Ferramenta Utilizada |
| :--- | :---: | :--- | :--- |
| **Especificação & Documentação** | Alto | Leitura da especificação da Parte 2 e estruturação das políticas de integridade | Antigravity AI (Gemini 2.5 Flash) |
| **Arquitetura de Sockets UDP** | Planejado | Auxílio na modelagem de estruturas de datagramas e protocolos de mensagens | — |
| **Codificação (Cluster/RMs/FE)** | Planejado | Sugestão de esqueletos de classes, buffers de reordenação e rotinas de eleição | — |
| **Testes Unitários & Carga** | Planejado | Elaboração de suítes de testes automatizados com simulação de perda/reordenação UDP | — |
| **Automação (Scripts/Makefile)** | Planejado | Scripts de inicialização do cluster, simulação de falhas e compilação | — |

---

## 4. Histórico de Sessões

Todas as sessões de interação detalhadas (prompts, ações executadas, artefatos gerados e validações) são registradas em:

👉 **[ai-sessions.md](ai-sessions.md)**
