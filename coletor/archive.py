"""Arquivo diário: move dias fechados do branch `state` para o branch `data`.

Modos:
  pending  -> escreve `pending=N` em $GITHUB_OUTPUT (N = dias por arquivar)
  export   -> converte os dias fechados para Parquet em DATA_DIR/<ano>/...
  cleanup  -> apaga do estado os ficheiros exportados (só após push com sucesso)
"""
import gzip
import hashlib
import json
import os
import shutil
import sys
from datetime import date, datetime, timedelta

import pandas as pd

from . import config as C

# Só estas pastas são arquivadas; ficheiros da raiz do estado (ex.: raw_infra_semana.txt) ficam.
KINDS = {"events": "csv", "samples": "csv", "static_events": "csv", "tariffs": "csv", "raw": "copy", "raw_infra": "copy"}
MANIFEST = C.STATE_DIR / ".archive_manifest.json"
MANIFEST_HASH = C.STATE_DIR / ".static_hash_pending.txt"


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
    done = []
    for day in closed_days():
        year = day[:4]
        for kind, how in KINDS.items():
            for src in sorted((C.STATE_DIR / kind).glob(f"{day}*")):
                dst_dir = C.DATA_DIR / year / kind
                dst_dir.mkdir(parents=True, exist_ok=True)
                if how == "copy":
                    shutil.copy2(src, dst_dir / src.name)
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
                    df.to_parquet(dst_dir / f"{stem}.parquet", index=False, compression="zstd")
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
                dst = C.DATA_DIR / f"{now:%Y}" / sub
                dst.mkdir(parents=True, exist_ok=True)
                df = pd.read_csv(f_, dtype=str, compression="gzip")
                for col in ("lat", "lon", "max_power_raw", "available_power_raw", "n_connectors", "voltage_max",
                            "current_max", "parking_spaces", "pmr_spaces", "simultaneous_points", "n_points",
                            "green_energy_share", "max_weight_t", "max_height_m", "max_length_m", "max_width_m"):
                    if col in df:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                df.to_parquet(dst / f"{now:%Y-%m-%dT%H%M}.parquet", index=False, compression="zstd")
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
