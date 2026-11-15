"""Arquivo diário: move dias fechados do branch `state` para o branch `data`.

Modos:
  pending  -> escreve `pending=N` em $GITHUB_OUTPUT (N = dias por arquivar)
  export   -> converte os dias fechados para Parquet em DATA_DIR/<ano>/...
  cleanup  -> apaga do estado os ficheiros exportados (só após push com sucesso)

Um ficheiro já arquivado nunca é substituído. Um ficheiro tardio (o dia já existe no branch
`data` com outro conteúdo) vai para _tardios/<ano>/<tipo>/, fora das pastas lidas por data,
e gera um aviso no resumo. Conteúdo igual ao já arquivado não grava nada.
"""
import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta

import pandas as pd

from . import config as C

# Só estas pastas são arquivadas; ficheiros da raiz do estado (ex.: raw_infra_semana.txt) ficam.
KINDS = {"events": "csv", "samples": "csv", "static_events": "csv", "tariffs": "csv", "raw": "copy", "raw_infra": "copy"}
MANIFEST = C.STATE_DIR / ".archive_manifest.json"
MANIFEST_HASH = C.STATE_DIR / ".static_hash_pending.txt"


TARDIOS = "_tardios"
_NA_ARVORE = {}  # cache de ls-tree por pasta (o HEAD do clone não muda durante a exportação)


def blob_sha(dados):
    """Hash de blob do git (o mesmo de `git hash-object`), calculado sem ler o repositório."""
    return hashlib.sha1(b"blob %d\0" % len(dados) + dados).hexdigest()


def na_arvore(rel_dir):
    """{nome: hash} dos ficheiros de DATA_DIR/rel_dir no commit atual do branch data.

    O clone é esparso (--sparse, --filter=blob:none): os ficheiros já arquivados não estão no
    disco, mas as árvores estão, por isso basta o ls-tree.
    """
    if not (C.DATA_DIR / ".git").exists():
        return {}
    if rel_dir not in _NA_ARVORE:
        r = subprocess.run(["git", "-C", str(C.DATA_DIR), "ls-tree", "-z", "HEAD", "--", rel_dir + "/"],
                           capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"git ls-tree falhou em {rel_dir} (código {r.returncode})")
        out = {}
        for item in r.stdout.decode("utf-8").split("\0"):
            if not item:
                continue
            meta, caminho = item.split("\t", 1)
            _modo, tipo, sha = meta.split()
            if tipo == "blob":
                out[caminho.rsplit("/", 1)[-1]] = sha
        _NA_ARVORE[rel_dir] = out
    return _NA_ARVORE[rel_dir]


def existentes(rel_dir):
    """{nome: hash}: ficheiros no commit atual mais os já gravados no disco nesta exportação."""
    out = dict(na_arvore(rel_dir))
    d = C.DATA_DIR / rel_dir
    if d.exists():
        for p in d.iterdir():
            if p.is_file():
                out[p.name] = blob_sha(p.read_bytes())
    return out


def avisar(msg):
    print(f"::warning::{msg}")
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"> ⚠️ {msg}\n")


def gravar(rel_dir, nome, dados):
    """Grava DATA_DIR/rel_dir/nome sem nunca substituir um ficheiro já arquivado.

    Devolve o caminho relativo gravado, ou None se o mesmo conteúdo já estiver arquivado.
    """
    sha = blob_sha(dados)
    principal = existentes(rel_dir)
    if nome not in principal:
        destino = f"{rel_dir}/{nome}"
    elif principal[nome] == sha:
        return None
    else:
        tard_dir = f"{TARDIOS}/{rel_dir}"
        tard = existentes(tard_dir)
        cab, _, resto = nome.partition(".")
        n = 1
        while True:
            cand = f"{cab}.tardio-{n}.{resto}"
            if cand not in tard:
                break
            if tard[cand] == sha:
                return None
            n += 1
        destino = f"{tard_dir}/{cand}"
        avisar(f"Arquivo: ficheiro tardio de {rel_dir.split('/')[-1]} {cab} gravado como "
               f"{destino}; o dia arquivado não foi substituído.")
    p = C.DATA_DIR / destino
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(dados)
    return destino


def parquet_bytes(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="zstd")
    return buf.getvalue()


def closed_days():
    cutoff = (datetime.now(C.TZ) - timedelta(minutes=C.ARCHIVE_GRACE_MIN)).date()
    days = set()
    for kind in KINDS:
        d = C.STATE_DIR / kind
        if d.exists():
            days |= {p.name[:10] for p in d.iterdir() if p.is_file()}
    return sorted(x for x in days if date.fromisoformat(x) < cutoff)


def pending():
    n = len(closed_days())
    print(f"Dias por arquivar: {n}")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as f:
            f.write(f"pending={n}\n")


def export():
    _NA_ARVORE.clear()
    done = []
    for day in closed_days():
        year = day[:4]
        for kind, how in KINDS.items():
            for src in sorted((C.STATE_DIR / kind).glob(f"{day}*")):
                rel_dir = f"{year}/{kind}"
                if how == "copy":
                    gravar(rel_dir, src.name, src.read_bytes())
                else:
                    df = pd.read_csv(src, dtype=str, compression="gzip", keep_default_na=False)
                    for col in ("ts_utc", "pub_utc", "fetch_utc"):
                        if col in df:
                            df[col] = pd.to_datetime(df[col].replace("", None), utc=True,
                                                     format="ISO8601")
                    for col in ("ok", "http", "n_points", "n_events", "bytes", "present"):
                        if col in df:
                            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                    for col in ("age_s", "wait_s", "secs", "eur_sessao", "eur_kwh", "eur_min"):
                        if col in df:
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                    # 2026-09-24.csv.gz -> 2026-09-24.parquet ; 2026-09-24.v1.csv.gz -> 2026-09-24.v1.parquet
                    stem = src.name[:-len(".csv.gz")]
                    gravar(rel_dir, f"{stem}.parquet", parquet_bytes(df))
                done.append(str(src))

    # Instantâneo do inventário (pontos e locais), só quando muda.
    sp = C.STATE_DIR / "static_points.csv.gz"
    ss = C.STATE_DIR / "static_sites.csv.gz"
    hp = C.STATE_DIR / "static_archived_hash.txt"
    if sp.exists() and done:
        h = hashlib.sha256()
        for f_ in (sp, ss):
            if f_.exists():
                with gzip.open(f_, "rb") as f:
                    h.update(f.read())
        h = h.hexdigest()
        if not hp.exists() or hp.read_text().strip() != h:
            now = datetime.now(C.TZ)
            for f_, sub in ((sp, "static"), (ss, "static_sites")):
                if not f_.exists():
                    continue
                df = pd.read_csv(f_, dtype=str, compression="gzip")
                for col in ("lat", "lon", "max_power_raw", "available_power_raw", "n_connectors", "voltage_max",
                            "current_max", "parking_spaces", "pmr_spaces", "simultaneous_points", "n_points",
                            "green_energy_share", "max_weight_t", "max_height_m", "max_length_m", "max_width_m"):
                    if col in df:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                gravar(f"{now:%Y}/{sub}", f"{now:%Y-%m-%dT%H%M}.parquet", parquet_bytes(df))
            MANIFEST_HASH.write_text(h)

    MANIFEST.write_text(json.dumps(done))
    print(f"Exportados {len(done)} ficheiros.")



def cleanup():
    if not MANIFEST.exists():
        return
    for p in json.loads(MANIFEST.read_text()):
        try:
            os.remove(p)
        except FileNotFoundError:
            pass
    MANIFEST.unlink()
    if MANIFEST_HASH.exists():
        MANIFEST_HASH.replace(C.STATE_DIR / "static_archived_hash.txt")
    print("Estado limpo.")


if __name__ == "__main__":
    {"pending": pending, "export": export, "cleanup": cleanup}[sys.argv[1]]()
