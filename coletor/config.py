"""Configuração central do coletor."""
import os
from pathlib import Path
from zoneinfo import ZoneInfo

# Feeds públicos NAP/MOBI.E (DATEX II)
STATUS_URL = os.environ.get("STATUS_URL", "https://pgm.mobie.pt/integration/nap/evActualStatus")
INFRA_URL = os.environ.get("INFRA_URL", "https://pgm.mobie.pt/integration/nap/evChargingInfra")

TZ = ZoneInfo("Europe/Lisbon")

STATE_DIR = Path(os.environ.get("STATE_DIR", "state"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))

# Recolhas mais próximas do que isto são ignoradas (evita duplicados
# quando o cron-job.org e o schedule do GitHub disparam em simultâneo).
# Um disparo duplicado que passe o intervalo é travado pela deteção de
# versão repetida (ETag + publicationTime, collect.py).
MIN_INTERVAL_S = 90

# Periodicidade de atualização do inventário estático.
STATIC_REFRESH_H = 6

# Orçamento de tempo do inventário (o job tem 12 min). Se a execução já esperou pela versão
# seguinte do feed ou já passaram mais de STATIC_MAX_START_S, o inventário fica para a execução
# seguinte. Um pedido por tentativa, até INFRA_ATTEMPTS tentativas com INFRA_RETRY_WAIT_S de pausa;
# nenhuma tentativa começa depois de INFRA_DEADLINE_S desde o início da execução.
STATIC_MAX_START_S = 180
INFRA_ATTEMPTS = 2
INFRA_RETRY_WAIT_S = 20
INFRA_DEADLINE_S = 420

# Um dia só é arquivado depois das 00:10 (Lisboa) do dia seguinte.
ARCHIVE_GRACE_MIN = 10

# Se o feed trouxer menos do que esta fração das tomadas conhecidas,
# é tratado como falha do feed (evita marcar milhares de tomadas como ausentes).
MIN_FEED_RATIO = 0.5

# Estado registado quando uma tomada deixa de constar do feed dinâmico.
ABSENT = "__AUSENTE__"

# Pedidos condicionais com ETag (If-None-Match). Medição de 23–24/09: 1 ETag por versão
# do feed em 164 de 164 casos, por isso é seguro evitar descarregar versões repetidas.
USE_CONDITIONAL = True

# Ciclo de publicação medido: o feed é gerado a cada 5 min (~4 s após cada múltiplo de 5 min)
# e só fica completo para download ~45–70 s depois. Se um run receber a versão já registada,
# espera pela seguinte (+ margem) e tenta uma vez mais.
PUBLISH_EVERY_S = 300
READY_MARGIN_S = 90
MAX_WAIT_S = 180

# Cópia bruta diária do XML (para reprocessamento) só durante o piloto.
RAW_DAILY_UNTIL = "2026-10-07"

# O Summary dos runs é público (repositório público): só mostra indicadores técnicos.
PUBLIC_SUMMARY = os.environ.get("PUBLIC_SUMMARY", "1") == "1"

USER_AGENT = "disponibilidade-carregamento-pt/1.0 (projeto independente; fonte NAP/MOBI.E)"
HTTP_TIMEOUT = (15, 120)
HTTP_RETRIES = 3
