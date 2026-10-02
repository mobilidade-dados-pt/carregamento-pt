# carregamento-pt — coletor de dados públicos de carregamento

> Ficheiro de contexto para o Claude Code. **Repositório público:** não acrescentar aqui análises, conclusões nem nomes de repositórios privados.

## O que faz

Recolhe, a cada 5 minutos, o estado e o inventário dos pontos de carregamento públicos em Portugal, a partir do Ponto de Acesso Nacional (NAP Portugal, IMT), com dados da rede MOBI.E em formato DATEX II. Condições de uso: livre acesso, com menção de fonte.

## Estrutura

- `coletor/collect.py`: recolha, deteção de mudanças, tarifários, inventário, relatório de preenchimento.
- `coletor/parse.py`: leitura do DATEX II, independente de namespaces.
- `coletor/archive.py`: arquivo diário em Parquet.
- `coletor/config.py`: parâmetros.
- `.github/workflows/recolha.yml`: execução; os dados são gravados no repositório indicado pelo segredo `DATA_REPO`, com o segredo `DATA_TOKEN`. Os dois só entram no `env` dos passos que os usam; o `DATA_URL` é montado dentro de cada script e nunca fica num bloco `env`.

## Regras

- Os dados **nunca** são gravados neste repositório.
- O resumo de cada execução (`GITHUB_STEP_SUMMARY`) só tem indicadores técnicos (`PUBLIC_SUMMARY=1`).
- O `checkout` usa `persist-credentials: false`. Sem isto, o token do repositório público sobrepõe-se ao `DATA_TOKEN`: foi a causa da perda de dados de 24–26/09.
- Um erro de acesso ao estado **falha a execução**. Nunca recomeçar do zero por cima de um estado existente.
- Ciclo do feed: publicado a cada 5 min (~4 s após cada múltiplo de 5), completo ~45–70 s depois; disparo nos minutos 2, 7, 12…; espera pela versão seguinte se a recebida for repetida; ETag ativo.
- Chave dos pontos: ID do feed, ou `local|ID` quando o ID se repete noutro local.
- Falha do inventário (truncado, sem resposta ou erro): **mantém-se o anterior**, com aviso no resumo; a execução não falha nem grava uma segunda amostra; a execução seguinte volta a tentar.
- Orçamento de tempo (job de 12 min): o inventário só é atualizado se a execução não esperou pela versão seguinte e tem menos de 180 s; 1 pedido por tentativa, até 2 tentativas, nenhuma começa depois de 420 s.
- "Guardar estado" corre com `!cancelled()`: um job cancelado (incluindo por limite de tempo) não publica estado. O push do estado tem até 3 novas tentativas (10, 20 e 40 s); o passo só falha depois da última.
- Cópia bruta do inventário (`raw_infra`): uma por semana ISO (hora de Lisboa). A semana da última cópia fica em `raw_infra_semana.txt`, na raiz do estado; o arquivo diário não o arquiva nem apaga.
- Gravações atómicas: temporário em `STATE_DIR/.tmp/` + `os.replace`; os `.csv.gz` diários são copiados com o novo membro antes de substituir. Nunca escrever diretamente nos ficheiros do estado.
- Testes: `python -m unittest discover tests -v` (amostras sintéticas em `tests/amostras/`, sem dados reais).
- Testar alterações com ficheiros de amostra antes de alterar o workflow. Comentários e mensagens em PT-PT.
