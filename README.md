# Disponibilidade da rede de carregamento — Portugal

Projeto independente. Fonte: NAP/MOBI.E (DATEX II).

Este repositório recolhe, a cada 5 minutos, o estado de todas as tomadas públicas da rede nacional e guarda o histórico para cálculo de disponibilidade e ocupação. O dashboard será acrescentado numa fase seguinte.

## Como funciona

| Componente | Função |
|---|---|
| `coletor/collect.py` | Descarrega o feed de estado. Regista apenas **mudanças de estado** (eventos) e um registo de cada amostra, para medir a cobertura e aplicar o teto de 10 min. Atualiza o inventário estático a cada 6h. |
| `coletor/archive.py` | Depois das 00:10 (Lisboa), converte o dia anterior para Parquet e move-o para o branch `data`. |
| `.github/workflows/recolha.yml` | Corre o coletor. É disparado pelo cron-job.org, com o schedule do GitHub como redundância. |

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

**Settings → Actions → General → Workflow permissions →** selecionar **Read and write permissions → Save**.

### 3. Primeiro run (manual)

1. Separador **Actions**. Se pedir, clicar em **I understand my workflows, go ahead and enable them**.
2. Abrir **Recolha → Run workflow → Run workflow**.
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
| Request body | `{"ref":"main"}` |

Resposta esperada: **204**.

## Verificação

- **Após 1 hora:** o separador Actions deve ter cerca de 12 runs com sucesso, e o branch `state` deve existir.
- **Depois das 00:10:** deve aparecer o branch `data`, com a pasta do ano.

## Notas

- **Recolhas duplicadas:** se o cron-job.org e o schedule do GitHub dispararem quase em simultâneo, a segunda recolha é ignorada (intervalo mínimo de 150 s).
- **Falhas do feed:** um feed indisponível, vazio ou com menos de 50% das tomadas conhecidas é registado como falha. Esse período conta como "sem dados" no cálculo.
- **Inatividade:** o GitHub desativa o schedule em repositórios sem atividade durante 60 dias. O disparo via cron-job.org não é afetado.
