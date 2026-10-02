"""Recolha: descarrega o estado das tomadas, regista transições e amostras.

Guarda apenas MUDANÇAS de estado (eventos) + um registo de cada amostra
(para calcular cobertura e aplicar o teto de 10 min no cálculo).
"""
import csv
import gzip
import io
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import requests

from . import config as C
from .parse import INFRA_FIELDS, SITE_FIELDS, fill_rates, parse_infra, parse_status, tariff_components, unique_keys

SUMMARY = []
SAMPLE_WRITTEN = False  # já foi gravada a amostra desta execução (evita uma segunda linha "falha")


def say(line=""):
    print(line)
    SUMMARY.append(line)


def write_summary():
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(SUMMARY) + "\n")


def md(x):
    return str(x).replace("|", "\\|")


def erro_curto(e):
    """Tipo e mensagem de um erro, numa linha e sem conteúdo do feed (o resumo é público).

    FeedError já traz uma mensagem construída pelo coletor: não se repete o tipo.
    """
    msg = (str(e).strip().splitlines() or [""])[0]
    txt = msg if isinstance(e, FeedError) else f"{type(e).__name__}: {msg}"
    return md(txt[:300])


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def lisbon_date(dt):
    return dt.astimezone(C.TZ).date().isoformat()


HEADER_COLS = [("Date", "h_date"), ("Last-Modified", "h_last_modified"),
               ("ETag", "h_etag"), ("Age", "h_age"), ("Cache-Control", "h_cache_control"),
               ("Expires", "h_expires")]
CACHE_HINTS = ("X-Cache", "CF-Cache-Status", "Via", "X-Proxy-Cache", "X-Cache-Status")


def pick_headers(h):
    out = {col: h.get(name, "") for name, col in HEADER_COLS}
    out["h_cache"] = "; ".join(f"{k}={h[k]}" for k in CACHE_HINTS if k in h)
    return out


def fetch(url, extra_headers=None, retries=None):
    """Devolve (conteúdo, código HTTP, cabeçalhos relevantes, início do pedido)."""
    last = None
    retries = retries or C.HTTP_RETRIES
    headers = {"User-Agent": C.USER_AGENT, "Accept-Encoding": "gzip", **(extra_headers or {})}
    for attempt in range(retries):
        start = datetime.now(timezone.utc)
        try:
            r = requests.get(url, timeout=C.HTTP_TIMEOUT, headers=headers)
            if r.status_code != 304:
                r.raise_for_status()
            return r.content, r.status_code, pick_headers(r.headers), start
        except Exception as e:  # noqa: BLE001
            last = e
            if attempt < retries - 1:  # sem pausa depois da última tentativa
                time.sleep(5 * (attempt + 1) ** 2)
    raise RuntimeError(f"{url}: {last}")


def parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def iso_ms(dt):
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z") if dt else ""


class FeedError(Exception):
    pass


@contextmanager
def atomic_write(path, mode="wb"):
    """Grava num temporário e só no fim substitui o ficheiro final (os.replace é atómico).

    Um corte a meio deixa o ficheiro anterior intacto. Os temporários ficam em STATE_DIR/.tmp/
    (mesmo disco, fora das pastas por data que o arquivo percorre).
    mode: "wb" (binário), "w" (texto), "gz" (texto gzip), "gzb" (binário gzip).
    """
    tmp_dir = C.STATE_DIR / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=tmp_dir, prefix=path.name + ".")
    os.close(fd)
    try:
        if mode == "gz":
            f = gzip.open(tmp, "wt", encoding="utf-8", newline="")
        elif mode == "gzb":
            f = gzip.open(tmp, "wb")
        elif mode == "w":
            f = open(tmp, "w", encoding="utf-8", newline="")
        else:
            f = open(tmp, "wb")
        with f:
            yield f
        with open(tmp, "rb") as g:
            os.fsync(g.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def save_debug(raw, err):
    """Guarda início e fim de uma resposta inválida, para diagnóstico."""
    d = C.STATE_DIR / "debug"
    d.mkdir(parents=True, exist_ok=True)
    head = raw[:1500].decode("utf-8", "replace")
    tail = raw[-1500:].decode("utf-8", "replace")
    with atomic_write(d / "ultima_resposta_invalida.txt", "w") as f:
        f.write(f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ}\n{err}\nbytes={len(raw)}\n"
                f"--- INÍCIO ---\n{head}\n--- FIM ---\n{tail}\n")


def fetch_parse_status(cond=None, attempts=2, wait_s=20):
    """Descarrega e lê o feed; repete se a resposta vier inválida (ex.: truncada).

    Devolve um dicionário com raw, http, hdr, start, rows, excerpt, pub, retries.
    Com resposta 304 (sem alterações), rows fica None.
    """
    last = None
    for i in range(attempts):
        try:
            raw, http, hdr, start = fetch(C.STATUS_URL, cond)
        except Exception as e:  # noqa: BLE001
            raise FeedError(f"download falhou: {e}") from e
        res = {"raw": raw, "http": http, "hdr": hdr, "start": start, "retries": i,
               "rows": None, "excerpt": None, "pub": None}
        if http == 304:
            return res
        try:
            rows, excerpt, pub = parse_status(io.BytesIO(raw))
            res.update(rows=rows, excerpt=excerpt, pub=parse_ts(pub))
            return res
        except Exception as e:  # noqa: BLE001 — XML inválido ou truncado
            last = f"XML inválido ({len(raw)/1e6:.1f} MB): {type(e).__name__}: {e}"
            save_debug(raw, last)
            if i < attempts - 1:
                time.sleep(wait_s)
    raise FeedError(last)


def fetch_parse_infra(deadline):
    """Descarrega e lê o inventário, com a mesma proteção do estado (resposta truncada ou falhada).

    Um pedido por tentativa; até INFRA_ATTEMPTS tentativas, com pausa entre elas, mas nenhuma
    começa depois de `deadline` (time.time()). Devolve (raw, pontos, excerto, locais).
    """
    last = "prazo esgotado"
    for i in range(C.INFRA_ATTEMPTS):
        if i:
            if time.time() + C.INFRA_RETRY_WAIT_S > deadline:
                last += " · sem tempo para nova tentativa"
                break
            time.sleep(C.INFRA_RETRY_WAIT_S)
        try:
            raw, _, _, _ = fetch(C.INFRA_URL, retries=1)
        except Exception as e:  # noqa: BLE001
            last = f"download falhou: {e}"
            continue
        try:
            rows, excerpt, sites = parse_infra(io.BytesIO(raw))
        except Exception as e:  # noqa: BLE001 — XML inválido ou truncado
            last = f"XML inválido ({len(raw)/1e6:.1f} MB): {type(e).__name__}: {e}"
            save_debug(raw, last)
            continue
        if not rows:
            last = f"inventário sem pontos lidos ({len(raw)/1e6:.1f} MB)"
            save_debug(raw, last)
            continue
        return raw, rows, excerpt, sites
    raise FeedError(last)


def load_json(path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(path, obj):
    with atomic_write(path, "w") as f:
        f.write(json.dumps(obj, ensure_ascii=False, indent=1))


def append_gz_csv(path, header, rows):
    """Acrescenta a um CSV gzip (membros gzip concatenados são válidos).

    O ficheiro é copiado com o novo membro para um temporário e só depois substituído:
    um corte a meio nunca deixa o ficheiro ilegível.
    Se o ficheiro existir com outro cabeçalho (mudança de versão do coletor),
    é renomeado para <nome>.vN.csv.gz e começa-se um ficheiro novo.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
            old_header = next(csv.reader(f), None)
        if old_header != list(header):
            stem = path.name[:-len(".csv.gz")]
            n = 1
            while (path.parent / f"{stem}.v{n}.csv.gz").exists():
                n += 1
            path.rename(path.parent / f"{stem}.v{n}.csv.gz")
    new = not path.exists()
    buf = io.StringIO()
    w = csv.writer(buf)
    if new:
        w.writerow(header)
    w.writerows(rows)
    old = b"" if new else path.read_bytes()
    with atomic_write(path, "wb") as f:
        f.write(old)
        f.write(gzip.compress(buf.getvalue().encode("utf-8")))


def load_last_tariffs():
    p = C.STATE_DIR / "last_tariffs.csv.gz"
    if not p.exists():
        return {}
    with gzip.open(p, "rt", encoding="utf-8", newline="") as f:
        return {r["point_id"]: r["tarifario"] for r in csv.DictReader(f)}


def save_last_tariffs(d):
    with atomic_write(C.STATE_DIR / "last_tariffs.csv.gz", "gz") as f:
        w = csv.writer(f)
        w.writerow(["point_id", "tarifario"])
        w.writerows(sorted(d.items()))


TARIFF_COLS = ["ts_utc", "point_id", "eur_sessao", "eur_kwh", "eur_min", "tarifario"]


def record_tariffs(rows, ev_dt):
    """Regista o tarifário de cada ponto quando muda (1.ª execução = tarifário de base)."""
    prev = load_last_tariffs()
    cur = {pid: tar for pid, _st, _lu, tar in rows}
    ts = iso(ev_dt)
    changes = []
    for pid, tar in cur.items():
        if prev.get(pid) != tar:
            fee, kwh, mins = tariff_components(tar, ts)
            changes.append((ts, pid, fee, kwh, mins, tar))
    if changes:
        append_gz_csv(C.STATE_DIR / "tariffs" / f"{lisbon_date(ev_dt)}.csv.gz", TARIFF_COLS, changes)
    merged = dict(prev)
    merged.update(cur)
    save_last_tariffs(merged)
    return len(changes), bool(prev)


def load_last_status():
    p = C.STATE_DIR / "last_status.csv.gz"
    if not p.exists():
        return {}
    with gzip.open(p, "rt", encoding="utf-8", newline="") as f:
        return {r["point_id"]: r["status"] for r in csv.DictReader(f)}


def save_last_status(d):
    with atomic_write(C.STATE_DIR / "last_status.csv.gz", "gz") as f:
        w = csv.writer(f)
        w.writerow(["point_id", "status"])
        w.writerows(sorted(d.items()))


SAMPLE_COLS = ["ts_utc", "ok", "http", "version", "pub_utc", "fetch_utc", "age_s", "wait_s",
               "n_points", "n_events", "bytes", "secs",
               "h_date", "h_last_modified", "h_etag", "h_age", "h_cache_control",
               "h_expires", "h_cache", "err"]


def record_sample(now, **kw):
    """Uma linha por run. version: nova | repetida | antiga | 304 | sem_pub | falha."""
    global SAMPLE_WRITTEN
    row = {c: "" for c in SAMPLE_COLS}
    row.update({"ts_utc": iso(now), "ok": 0, "n_points": 0, "n_events": 0, "bytes": 0,
                "version": "falha"})
    row.update({k: v for k, v in kw.items() if k in row})
    append_gz_csv(C.STATE_DIR / "samples" / f"{lisbon_date(now)}.csv.gz",
                  SAMPLE_COLS, [[row[c] for c in SAMPLE_COLS]])
    SAMPLE_WRITTEN = True


# ---------------------------------------------------------------- estático
RAW_INFRA_MARCADOR = "raw_infra_semana.txt"  # na raiz do estado: o arquivo diário não o toca


def refresh_static(now, t0, waited=False):
    """Atualiza o inventário se tiver mais de STATIC_REFRESH_H horas.

    Orçamento de tempo: se a execução já esperou pela versão seguinte do feed, ou se já passaram
    mais de STATIC_MAX_START_S, fica para a execução seguinte. Se o inventário falhar, mantém-se
    o anterior (static_meta.json não muda, por isso a execução seguinte tenta de novo).
    """
    meta_p = C.STATE_DIR / "static_meta.json"
    meta = load_json(meta_p, {})
    if meta.get("ts_utc"):
        age_h = (now - datetime.fromisoformat(meta["ts_utc"].replace("Z", "+00:00"))).total_seconds() / 3600
        if age_h < C.STATIC_REFRESH_H:
            return None
    elapsed = time.time() - t0
    if waited or elapsed > C.STATIC_MAX_START_S:
        say(f"- Inventário adiado para a execução seguinte (orçamento de tempo: "
            f"{'houve espera pela versão seguinte · ' if waited else ''}{elapsed:.0f} s decorridos).")
        return None
    try:
        raw, rows, excerpt, sites = fetch_parse_infra(t0 + C.INFRA_DEADLINE_S)
    except FeedError as e:
        say(f"> ⚠️ Inventário não atualizado: {erro_curto(e)}. Mantém-se o anterior; "
            "nova tentativa na execução seguinte.")
        return None

    # Eventos de presença no inventário (entrada/saída de tomadas).
    old_p = C.STATE_DIR / "static_points.csv.gz"
    old_ids = set()
    if old_p.exists():
        with gzip.open(old_p, "rt", encoding="utf-8", newline="") as f:
            old_ids = {r["point_id"] for r in csv.DictReader(f)}
    new_ids = {r["point_id"] for r in rows}
    ev = [(iso(now), pid, 1) for pid in sorted(new_ids - old_ids)]
    ev += [(iso(now), pid, 0) for pid in sorted(old_ids - new_ids)]
    if ev:
        append_gz_csv(C.STATE_DIR / "static_events" / f"{lisbon_date(now)}.csv.gz",
                      ["ts_utc", "point_id", "present"], ev)

    with atomic_write(old_p, "gz") as f:
        w = csv.DictWriter(f, fieldnames=INFRA_FIELDS)
        w.writeheader()
        w.writerows(rows)
    with atomic_write(C.STATE_DIR / "static_sites.csv.gz", "gz") as f:
        w = csv.DictWriter(f, fieldnames=SITE_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(sites)
    # preenchimento de cada atributo (para decidir o que é utilizável)
    fr_s, fr_p = fill_rates(sites, SITE_FIELDS), fill_rates(rows, INFRA_FIELDS)
    with atomic_write(C.STATE_DIR / "static_fillrate.csv", "w") as f:
        w = csv.writer(f); w.writerow(["nivel", "campo", "pct_preenchido"])
        w.writerows([("local", k, v) for k, v in fr_s.items()] + [("ponto", k, v) for k, v in fr_p.items()])
    say("\n### Preenchimento dos atributos do inventário (%)")
    say("| Nível | Campo | % |")
    say("|---|---|---|")
    for lvl, fr in (("local", fr_s), ("ponto", fr_p)):
        for k, v in fr.items():
            say(f"| {lvl} | `{k}` | {v} |")
    # cópia bruta semanal do inventário (reprocessamento de campos novos no futuro).
    # A semana ISO da última cópia fica em RAW_INFRA_MARCADOR: o arquivo diário tira as cópias
    # do estado, por isso a pasta raw_infra/ não serve para saber se a semana já tem cópia.
    day = lisbon_date(now)
    wk = now.astimezone(C.TZ).isocalendar()
    semana = f"{wk.year}-W{wk.week:02d}"
    marcador = C.STATE_DIR / RAW_INFRA_MARCADOR
    ultima = marcador.read_text(encoding="utf-8").strip() if marcador.exists() else ""
    if ultima != semana:
        rawdir = C.STATE_DIR / "raw_infra"
        # Ainda sem cópia desta semana no estado (verificação anterior ao marcador, para a transição).
        if not any(rawdir.glob(f"*W{wk.week:02d}*")) if rawdir.exists() else True:
            with atomic_write(rawdir / f"{day}.W{wk.week:02d}.infra.xml.gz", "gzb") as f:
                f.write(raw)
        with atomic_write(marcador, "w") as f:
            f.write(semana + "\n")
    save_json(meta_p, {"ts_utc": iso(now), "n_points": len(rows),
                       "n_sites": len({r["site_id"] for r in rows})})
    return rows, excerpt


def static_diagnostics(rows):
    n = len(rows)
    pw = sorted(r["max_power_raw"] for r in rows if r["max_power_raw"] is not None)
    coords = sum(r["lat"] is not None and r["lon"] is not None for r in rows)
    say("\n### Diagnóstico do inventário")
    say(f"- Com coordenadas: {coords/n:.1%} · com potência: {len(pw)/n:.1%}")
    if pw:
        q = lambda f: pw[min(len(pw) - 1, int(f * len(pw)))]
        say(f"- Potência bruta — mín {pw[0]:g} · p25 {q(.25):g} · mediana {q(.5):g} · "
            f"p75 {q(.75):g} · máx {pw[-1]:g}")
    ex = next((r["hours_raw"] for r in rows if r.get("hours_raw")), None)
    if ex:
        say(f"- Exemplo de horário restrito: `{md(ex[:300])}`")
    for field, label in (("connector_types", "Tipos de conector"),
                         ("charging_modes", "Modos de carregamento"),
                         ("hours_type", "Tipo de horário")):
        top = Counter(r[field] or "(vazio)" for r in rows).most_common(8)
        say(f"- {label}: " + " · ".join(f"`{md(k)}` {v}" for k, v in top))


def operators_report(rows):
    pts = Counter()
    sites = {}
    names = {}
    for r in rows:
        key = r["operator_id"] or "?"
        pts[key] += 1
        sites.setdefault(key, set()).add(r["site_id"])
        names[key] = r["operator_name"] or "?"
    total = sum(pts.values())
    out = C.STATE_DIR / "operators_ranking.csv"
    with atomic_write(out, "w") as f:
        w = csv.writer(f)
        w.writerow(["rank", "operator_id", "operator_name", "locais", "pontos", "pct_pontos"])
        for i, (k, n) in enumerate(pts.most_common(), 1):
            w.writerow([i, k, names[k], len(sites[k]), n, round(100 * n / total, 2)])
    if C.PUBLIC_SUMMARY:
        say(f"- Inventário: {len(pts)} operadores (ranking guardado no repositório de dados)")
        return
    say("\n### Ranking de operadores (top 25, por nº de pontos)")
    say("| # | Operador | Código | Locais | Pontos | % |")
    say("|---|---|---|---|---|---|")
    for i, (k, n) in enumerate(pts.most_common(25), 1):
        say(f"| {i} | {md(names[k])} | {md(k)} | {len(sites[k])} | {n} | {100*n/total:.1f}% |")


# ---------------------------------------------------------------- principal
def main():
    t0 = time.time()
    now = datetime.now(timezone.utc)
    C.STATE_DIR.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(C.STATE_DIR / ".tmp", ignore_errors=True)  # restos de uma execução cortada
    force = "--force" in sys.argv

    last = load_json(C.STATE_DIR / "last_run.json", {})
    if last.get("ts_utc") and not force:
        prev = datetime.fromisoformat(last["ts_utc"].replace("Z", "+00:00"))
        if (now - prev).total_seconds() < C.MIN_INTERVAL_S:
            print(f"Recolha anterior há {(now - prev).total_seconds():.0f}s — ignorada.")
            return 0
    last["ts_utc"] = iso(now)
    save_json(C.STATE_DIR / "last_run.json", last)

    say(f"## Recolha {now.astimezone(C.TZ):%Y-%m-%d %H:%M} (Lisboa)")

    # Ficheiros de versões anteriores que já não são usados.
    for old in ("latest_status.xml.gz", "latest_infra.xml.gz"):
        (C.STATE_DIR / old).unlink(missing_ok=True)

    try:
        return process(now, t0, last)
    except Exception as e:  # noqa: BLE001 — erro inesperado: regista e falha o run
        if not SAMPLE_WRITTEN:  # nunca uma segunda amostra para a mesma execução
            record_sample(now, err=f"erro interno: {type(e).__name__}: {e}"[:300],
                          secs=round(time.time() - t0, 1))
        say(f"> ❌ Erro interno: `{erro_curto(e)}`")
        if not C.PUBLIC_SUMMARY:
            say("```\n" + traceback.format_exc()[-3000:] + "\n```")
        raise


def lisbon_hms(dt):
    return dt.astimezone(C.TZ).strftime("%H:%M:%S") if dt else "—"


def process(now, t0, last):
    # Pedido condicional: se o servidor suportar, devolve 304 sem descarregar 36 MB.
    cond = {}
    if C.USE_CONDITIONAL and last.get("etag"):
        cond["If-None-Match"] = last["etag"]

    try:
        res = fetch_parse_status(cond)
    except FeedError as e:
        record_sample(now, err=str(e)[:300], secs=round(time.time() - t0, 1))
        say(f"> ❌ Feed dinâmico indisponível ou inválido: {erro_curto(e)}")
        return 0
    if res["retries"]:
        say(f"> ⚠️ Primeira resposta inválida; recuperado à tentativa {res['retries'] + 1}.")

    last_pub = parse_ts(last.get("pub_utc"))

    # Versão já registada (304 ou mesmo publicationTime): esperar pela próxima publicação e tentar uma vez.
    wait_s, waited = 0, False
    if last_pub and (res["http"] == 304 or (res["pub"] is not None and res["pub"] <= last_pub)):
        target = last_pub + timedelta(seconds=C.PUBLISH_EVERY_S + C.READY_MARGIN_S)
        while target <= datetime.now(timezone.utc):          # se já passou mais de um ciclo
            target += timedelta(seconds=C.PUBLISH_EVERY_S)
        wait = (target - datetime.now(timezone.utc)).total_seconds()
        if wait <= C.MAX_WAIT_S:
            say(f"- Versão já registada; a aguardar {wait:.0f} s pela publicação seguinte.")
            time.sleep(max(0, wait))
            wait_s, waited = round(wait), True
            try:
                res2 = fetch_parse_status(cond)
                if res2["http"] != 304 and res2["pub"] is not None and res2["pub"] > last_pub:
                    res = res2
                    say("- Nova versão obtida após a espera.")
            except FeedError as e:
                say(f"> ⚠️ Nova tentativa falhou: {erro_curto(e)}")

    raw, http, hdr, start, pub = res["raw"], res["http"], res["hdr"], res["start"], res["pub"]
    age = round((start - pub).total_seconds(), 1) if pub else ""
    base = dict(http=http, fetch_utc=iso_ms(start), pub_utc=iso_ms(pub), age_s=age,
                bytes=len(raw), wait_s=wait_s, **hdr)

    # Classificação da versão recebida.
    if http == 304:
        version = "304"
        pub = last_pub
    elif pub is None:
        version = "sem_pub"
    elif last_pub and pub == last_pub:
        version = "repetida"
    elif last_pub and pub < last_pub:
        version = "antiga"
    else:
        version = "nova"

    say(f"- Versão do feed: **{version}** · gerada às {lisbon_hms(pub)} · "
        f"pedida às {lisbon_hms(start)} · idade no download: {age if age != '' else '—'} s")
    cache_bits = [f"{k[2:]}={v}" for k, v in hdr.items() if v and k != "h_date"]
    say(f"- Cabeçalhos: {md(' · '.join(cache_bits)) or '(sem cabeçalhos de cache)'}")

    if version in ("304", "repetida", "antiga"):
        # Mesma informação já registada: não é uma observação nova.
        record_sample(now, ok=1, version=version, n_points=len(set(unique_keys([(r[4], r[0]) for r in res["rows"] or []]))),
                      secs=round(time.time() - t0, 1), **base)
        say("- Sem observação nova (versão já registada).")
        return 0

    rows, excerpt = res["rows"], res["excerpt"]
    if not (C.STATE_DIR / "last_status.csv.gz").exists():
        say("> ⚠️ **Estado anterior vazio**: esta execução recomeça o registo (todos os pontos como eventos). "
            "Só é normal na primeira execução de sempre; se aparecer outra vez, há um problema na leitura do estado.")
    # IDs repetidos entre locais diferentes (operadores com IDs simples): chave "local|ID"
    keys = unique_keys([(r[4], r[0]) for r in rows])
    n_coll = sum(1 for k in keys if "|" in k)
    rows = [(k, r[1], r[2], r[3]) for k, r in zip(keys, rows)]
    prev = load_last_status()
    cur = {}
    dups = {}
    for pid, st, _lu, _tar in rows:
        if pid in cur:
            dups.setdefault(pid, [cur[pid]]).append(st)
        cur[pid] = st
    dup = len(dups)

    if not cur or (prev and len(cur) < C.MIN_FEED_RATIO * sum(v != C.ABSENT for v in prev.values())):
        record_sample(now, n_points=len(cur), err="feed vazio ou parcial",
                      secs=round(time.time() - t0, 1), **base)
        say(f"> ❌ Feed vazio ou parcial ({len(cur)} pontos). Tratado como falha (sem dados).")
        if not cur and not C.PUBLIC_SUMMARY:
            say("```xml\n" + raw[:2500].decode("utf-8", "replace") + "\n```")
        return 0

    # Hora do evento = hora de geração do feed (publicationTime); se faltar, hora do pedido.
    ev_dt = pub or start
    ev_ts = iso(ev_dt)
    events = [(ev_ts, pid, st) for pid, st in cur.items() if prev.get(pid) != st]
    events += [(ev_ts, pid, C.ABSENT) for pid, st in prev.items()
               if pid not in cur and st != C.ABSENT]
    new_last = {pid: C.ABSENT for pid in prev}
    new_last.update(cur)

    if events:
        append_gz_csv(C.STATE_DIR / "events" / f"{lisbon_date(ev_dt)}.csv.gz",
                      ["ts_utc", "point_id", "status"], events)
    save_last_status(new_last)
    n_tar, had_tar = record_tariffs(rows, ev_dt)

    day = lisbon_date(now)
    raw_day = C.STATE_DIR / "raw" / f"{day}.status.xml.gz"
    if day <= C.RAW_DAILY_UNTIL and not raw_day.exists():  # 1 cópia bruta por dia
        with atomic_write(raw_day, "gzb") as f:
            f.write(raw)

    record_sample(now, ok=1, version=version, n_points=len(cur), n_events=len(events),
                  secs=round(time.time() - t0, 1), **base)
    last.update(pub_utc=iso_ms(pub), etag=hdr.get("h_etag", ""),
                last_modified=hdr.get("h_last_modified", ""))
    save_json(C.STATE_DIR / "last_run.json", last)

    say(f"- Pontos no feed: **{len(cur)}** · eventos (mudanças): **{len(events)}** · "
        f"IDs partilhados entre locais: {n_coll} (chave local|ID) · duplicados: {dup} · {len(raw)/1e6:.1f} MB · {time.time()-t0:.1f}s")
    if dups and not C.PUBLIC_SUMMARY:
        conflict = {k: v for k, v in dups.items() if len(set(v)) > 1}
        say(f"- IDs repetidos: {len(dups)} · com estados diferentes: {len(conflict)}")
        say("\n<details><summary>Amostra de IDs repetidos</summary>\n")
        for k, v in list(dups.items())[:15]:
            say(f"- `{k}`: {', '.join(v)}")
        say("</details>\n")
    say(f"- Tarifários: **{n_tar}** {'alterações registadas' if had_tar else 'pontos no registo de base'}")
    if not C.PUBLIC_SUMMARY:
        say("\n### Estados no feed")
        say("| Estado | Pontos |")
        say("|---|---|")
        for st, n in Counter(cur.values()).most_common():
            say(f"| `{st or '(vazio)'}` | {n} |")

    # O estado já está gravado: um problema no inventário nunca faz falhar a execução.
    try:
        static = refresh_static(now, t0, waited)
        if static:
            srows, sexcerpt = static
            sids = {r["point_id"] for r in srows}
            in_static = sum(pid in sids for pid in cur) / len(cur)
            in_dyn = sum(pid in cur for pid in sids) / len(sids)
            say(f"\n- Inventário: {len(srows)} pontos · dinâmico∩estático: "
                f"{in_static:.1%} dos pontos dinâmicos, {in_dyn:.1%} dos estáticos")
            if not C.PUBLIC_SUMMARY:
                static_diagnostics(srows)
            operators_report(srows)
            if not prev and not C.PUBLIC_SUMMARY:  # primeira execução: mostrar estrutura para validação
                say("\n<details><summary>Excerto XML — estado</summary>\n\n```xml\n"
                    f"{excerpt}\n```\n</details>")
                say("\n<details><summary>Excerto XML — inventário</summary>\n\n```xml\n"
                    f"{sexcerpt}\n```\n</details>")
    except Exception as e:  # noqa: BLE001
        say(f"> ⚠️ Inventário não atualizado ({erro_curto(e)}). Mantém-se o anterior.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        write_summary()
