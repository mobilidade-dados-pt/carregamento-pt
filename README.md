# Disponibilidade da rede de carregamento — Portugal

Projeto independente. Fonte: NAP/MOBI.E (DATEX II).

Este repositório recolhe, a cada 5 minutos, o estado de todas as tomadas públicas da rede nacional e guarda o histórico para cálculo de disponibilidade e ocupação. O dashboard já existe e usa estes dados.

## Como funciona

| Componente | Função |
|---|---|
| `coletor/collect.py` | Descarrega o feed de estado. Regista apenas **mudanças de estado** (eventos) e um registo de cada amostra, para medir a cobertura e aplicar o teto de 10 min. Atualiza o inventário estático a cada 6h. |
| `coletor/archive.py` | Depois das 00:10 (Lisboa), converte o dia anterior para Parquet e move-o para o branch `data`. Nunca substitui um ficheiro já arquivado. |
| `.github/workflows/recolha.yml` | Corre o coletor. Só tem `workflow_dispatch`: é disparado pelos disparadores externos, com a entrada `origem` (por omissão `externo`). |
| `.github/workflows/reserva.yml` | Único workflow agendado (a cada 5 min). Se a última recolha tiver mais de 360 s, dispara-a com origem `reserva`. A entrada `forcar` dispara a recolha num teste manual. |

Dados:

- Este repositório contém **apenas o código** da recolha.
- O estado corrente e o arquivo diário (Parquet) são gravados num **repositório privado**, indicado pelo segredo `DATA_REPO` e acedido com o segredo `DATA_TOKEN`.
- O Summary de cada run mostra apenas indicadores técnicos.

Fonte dos dados: Ponto de Acesso Nacional (NAP Portugal, IMT) — dados da rede MOBI.E, livre acesso.

## Instalação (uma vez)

### 1. Criar o repositório

1. No GitHub: **New repository**. Nome sugerido: `disponibilidade-carregamento-pt`. Visibilidade **Public** (obrigatório para minutos de Actions ilimitados).
2. Carregar o conteúdo deste zip: **Add file → Upload files**, arrastar todas as pastas e ficheiros e fazer commit em `main`.
   - Atenção: a pasta `.github` é oculta. No macOS, `Cmd + Shift + .` mostra-a no Finder.
   - Alternativa: **Add file → Create new file**, escrever o caminho `.github/workflows/recolha.yml` e colar o conteúdo.

### 2. Permissões

**Settings → Actions → General → Workflow permissions →** selecionar **Read repository contents and packages permissions** (só leitura) **→ Save**.

Cada workflow declara as suas permissões: `contents: read` na recolha e `actions: write` na reserva (só para disparar a recolha). Os dados são gravados com o segredo `DATA_TOKEN`, não com o token do workflow.

### 3. Primeiro run (manual)

1. Separador **Actions**. Se pedir, clicar em **I understand my workflows, go ahead and enable them**.
2. Abrir **Recolha → Run workflow**, deixar `origem` com o valor por omissão e clicar em **Run workflow**.
3. Quando terminar (cerca de 1–2 min), abrir o run e ler o **Summary**. Deve mostrar:
   - nº de pontos e tabela de estados;
   - ranking de operadores;
   - excertos XML (só no primeiro run).
4. **Copiar o Summary completo e enviá-lo.** Serve para validar o parser e fechar a lista de operadores.

### 4. Token para o cron-job.org

1. GitHub → foto de perfil → **Settings → Developer settings → Personal access tokens → Fine-grained tokens → Generate new token**.
2. Preencher:
   - **Repository access:** Only select repositories → este repositório.
   - **Permissions → Repository permissions → Actions:** Read and write.
   - **Expiration:** o prazo mais longo disponível.
3. **Criar um lembrete no calendário uma semana antes da expiração.** Um token expirado para o disparo e gera um erro 401.
4. Copiar o token (só é mostrado uma vez).

### 5. Agendamento no cron-job.org

Criar um cronjob com:

| Campo | Valor |
|---|---|
| URL | `https://api.github.com/repos/<utilizador>/<repositório>/actions/workflows/recolha.yml/dispatches` |
| Schedule | Every 5 minutes |
| Request method (Advanced) | POST |
| Headers | `Accept: application/vnd.github+json`<br>`Authorization: Bearer <token>`<br>`X-GitHub-Api-Version: 2022-11-28` |
| Request body | `{"ref":"main","inputs":{"origem":"cron-job.org"}}` |

Resposta esperada: **204**. Sem `inputs`, a recolha corre com origem `externo`. Uma entrada que o workflow não tenha é recusada (422): só acrescentar `origem` depois de o `recolha.yml` a ter.

## Verificação

- **Após 1 hora:** o separador Actions deve ter cerca de 12 runs com sucesso, e o branch `state` deve existir.
- **Depois das 00:10:** deve aparecer o branch `data`, com a pasta do ano.

## Notas

- **Recolhas duplicadas:** se dois disparos chegarem quase em simultâneo, a segunda recolha é ignorada (intervalo mínimo de 90 s) e não grava o estado nem arquiva. Um disparo duplicado que passe o intervalo é travado pela deteção de versão repetida (ETag e `publicationTime`): não gera observação nova.
- **Monitorização externa:** no fim de cada execução bem sucedida é feito um ping ao healthchecks.io, com o endereço guardado no segredo `HC_PING_URL_RECOLHA`. Se os pings faltarem, o healthchecks.io alerta. Sem o segredo, a execução só emite um aviso.
- **Falhas do feed:** um feed indisponível, vazio ou com menos de 50% das tomadas conhecidas é registado como falha. Esse período conta como "sem dados" no cálculo.
- **Inatividade:** num repositório público, o GitHub desativa os workflows agendados após 60 dias sem atividade. Só o `reserva.yml` é agendado. A recolha principal só tem `workflow_dispatch`, não é desativada e não depende da reserva. Se a reserva for desativada, reativar em **Actions → Reserva → Enable workflow**.
- **Ficheiros tardios:** se um dia já arquivado voltar a aparecer no estado com outro conteúdo, o ficheiro vai para `_tardios/<ano>/<tipo>/` no branch `data`, com `.tardio-N` no nome, e o resumo da execução mostra um aviso. As pastas `<ano>/<tipo>/` nunca são alteradas. Com o mesmo conteúdo, nada é gravado.
- **Prazo do feed:** cada descarga do feed de estado tem um prazo total de 240 s (todas as tentativas incluídas).
