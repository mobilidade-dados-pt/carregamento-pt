"""Testes de robustez da recolha, com ficheiros de amostra sintéticos (sem rede nem repositório de dados).

Cenários: (a) inventário truncado, (b) inventário sem resposta, (c) execução longa,
(d) corte a meio de uma gravação, (e) execução normal. Em todos: nenhuma amostra duplicada
e todos os ficheiros do estado legíveis.

Executar: python -m unittest discover tests -v
"""
import csv
import gzip
import io
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pandas as pd

from coletor import archive
from coletor import collect
from coletor import config as C

AMOSTRAS = Path(__file__).parent / "amostras"
INFRA = (AMOSTRAS / "infra.xml").read_bytes()
STATUS_TPL = (AMOSTRAS / "status.xml").read_text(encoding="utf-8")


def status_xml(pub, a1="available", a2="charging", b1="outOfOrder", c1="available"):
    s = STATUS_TPL.replace("{pub}", pub.strftime("%Y-%m-%dT%H:%M:%SZ"))
    for k, v in (("a1", a1), ("a2", a2), ("b1", b1), ("c1", c1)):
        s = s.replace("{st_" + k + "}", v)
    return s.encode("utf-8")


class Relogio:
    """Relógio simulado: time.time() e time.sleep() do coletor (as pausas fazem avançar o relógio)."""

    def __init__(self):
        self.t = 1_000_000.0

    def time(self):
        return self.t

    def sleep(self, s):
        self.t += max(0, s)


class Agora(datetime):
    """datetime.now() simulado: cada execução começa 5 min depois da anterior (como o cron)."""

    atual = None

    @classmethod
    def now(cls, tz=None):
        return cls.atual if tz is None else cls.atual.astimezone(tz)


class Feed:
    """Simula collect.fetch: devolve as respostas de estado e de inventário por ordem."""

    def __init__(self, relogio, status=(), infra=()):
        self.relogio = relogio
        self.status = list(status)
        self.infra = list(infra)
        self.pedidos = {"status": 0, "infra": 0}

    def __call__(self, url, extra_headers=None, retries=None):
        kind = "infra" if url == C.INFRA_URL else "status"
        self.pedidos[kind] += 1
        item = (self.infra if kind == "infra" else self.status).pop(0)
        if callable(item):
            item = item()
        if isinstance(item, Exception):
            raise item
        hdr = collect.pick_headers({"ETag": f'"{hash(item)}"'})
        return item, 200, hdr, datetime.now(timezone.utc)


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = Path(self.tmp.name) / "state"
        self.relogio = Relogio()
        self.patches = [
            mock.patch.object(C, "STATE_DIR", self.state),
            mock.patch.object(C, "MIN_INTERVAL_S", 0),
            mock.patch.object(C, "USE_CONDITIONAL", False),
            mock.patch.object(collect, "time", types.SimpleNamespace(time=self.relogio.time,
                                                                     sleep=self.relogio.sleep)),
            mock.patch.object(collect, "SUMMARY", []),
            mock.patch.object(collect, "datetime", Agora),
        ]
        Agora.atual = datetime.now(timezone.utc).replace(microsecond=0)
        for p in self.patches:
            p.start()
        collect.SAMPLE_WRITTEN = False
        self.pub0 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=30)

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        self.tmp.cleanup()

    # ---------------------------------------------------------------- auxiliares
    def proxima_execucao(self):
        Agora.atual += timedelta(minutes=5)

    def correr(self, feed):
        self.proxima_execucao()
        collect.SAMPLE_WRITTEN = False
        collect.SUMMARY.clear()
        with mock.patch.object(collect, "fetch", feed):
            rc = collect.main()
        self.assertEqual(rc, 0)
        return "\n".join(collect.SUMMARY)

    def primeira_execucao(self):
        """Estado de partida: uma execução normal, com inventário."""
        self.correr(Feed(self.relogio, status=[status_xml(self.pub0)], infra=[INFRA]))
        self.forcar_inventario_em_atraso()

    def forcar_inventario_em_atraso(self):
        meta = self.state / "static_meta.json"
        old = (datetime.now(timezone.utc) - timedelta(hours=C.STATIC_REFRESH_H + 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        collect.save_json(meta, {**collect.load_json(meta, {}), "ts_utc": old})

    def amostras(self):
        rows = []
        for p in sorted((self.state / "samples").glob("*.csv.gz")):
            with gzip.open(p, "rt", encoding="utf-8", newline="") as f:
                rows += list(csv.DictReader(f))
        return rows

    def static_bytes(self):
        return {n: (self.state / n).read_bytes() for n in
                ("static_points.csv.gz", "static_sites.csv.gz", "static_meta.json")}

    def verificar_estado(self, n_execucoes):
        """Nenhuma amostra duplicada, todos os ficheiros legíveis, sem temporários nas pastas por data."""
        am = self.amostras()
        self.assertEqual(len(am), n_execucoes, "uma e uma só amostra por execução")
        self.assertEqual(len({r["ts_utc"] for r in am}), len(am), "amostras com ts_utc duplicado")
        for p in self.state.rglob("*.csv.gz"):
            if ".tmp" in p.parts:
                continue
            with gzip.open(p, "rb") as f:
                f.read()  # gzip truncado -> EOFError
            pd.read_csv(p, dtype=str, compression="gzip", keep_default_na=False)
        tmp_dir = self.state / ".tmp"
        self.assertFalse(tmp_dir.exists() and any(tmp_dir.iterdir()), "temporários por limpar")
        for kind in archive.KINDS:
            d = self.state / kind
            if d.exists():
                for p in d.iterdir():
                    self.assertRegex(p.name, r"^\d{4}-\d{2}-\d{2}")
        archive.closed_days()  # não pode falhar
        return am


# ---------------------------------------------------------------- cenários
class TestRobustez(Base):
    def test_e_execucao_normal(self):
        self.primeira_execucao()
        am = self.verificar_estado(1)
        self.assertEqual(am[0]["version"], "nova")
        self.assertEqual(am[0]["ok"], "1")
        with gzip.open(self.state / "static_points.csv.gz", "rt", encoding="utf-8") as f:
            self.assertEqual(len(list(csv.DictReader(f))), 4)
        with gzip.open(next((self.state / "events").glob("*.csv.gz")), "rt", encoding="utf-8") as f:
            self.assertEqual(len(list(csv.DictReader(f))), 4)
        self.assertEqual(len(list((self.state / "raw_infra").iterdir())), 1)

    def test_a_inventario_truncado(self):
        self.primeira_execucao()
        antes = self.static_bytes()
        cortado = INFRA[: len(INFRA) // 2]
        feed = Feed(self.relogio, status=[status_xml(self.pub0 + timedelta(minutes=5), a1="charging")],
                    infra=[cortado, cortado])
        resumo = self.correr(feed)
        self.assertEqual(feed.pedidos["infra"], 2, "uma nova tentativa após a resposta truncada")
        self.assertIn("Inventário não atualizado", resumo)
        self.assertIn("XML inválido", resumo)
        self.assertEqual(self.static_bytes(), antes, "mantém-se o inventário anterior")
        self.assertTrue((self.state / "debug" / "ultima_resposta_invalida.txt").exists())
        am = self.verificar_estado(2)
        self.assertEqual(am[-1]["version"], "nova")
        self.assertEqual(am[-1]["ok"], "1")
        self.assertEqual(am[-1]["err"], "")

    def test_a2_inventario_truncado_recupera(self):
        self.primeira_execucao()
        feed = Feed(self.relogio, status=[status_xml(self.pub0 + timedelta(minutes=5))],
                    infra=[INFRA[: len(INFRA) // 2], INFRA])
        resumo = self.correr(feed)
        self.assertNotIn("Inventário não atualizado", resumo)
        self.assertIn("Inventário: 4 pontos", resumo)
        self.verificar_estado(2)

    def test_b_inventario_sem_resposta(self):
        self.primeira_execucao()
        antes = self.static_bytes()
        erro = RuntimeError(f"{C.INFRA_URL}: Read timed out. (read timeout=120)")

        def timeout():  # cada tentativa consome o tempo de leitura
            self.relogio.sleep(135)
            return erro

        feed = Feed(self.relogio, status=[status_xml(self.pub0 + timedelta(minutes=5))],
                    infra=[timeout, timeout])
        resumo = self.correr(feed)
        self.assertEqual(feed.pedidos["infra"], 2)
        self.assertIn("Inventário não atualizado", resumo)
        self.assertIn("timed out", resumo)
        self.assertEqual(self.static_bytes(), antes)
        am = self.verificar_estado(2)
        self.assertEqual(am[-1]["ok"], "1")

    def test_b2_sem_nova_tentativa_depois_do_prazo(self):
        self.primeira_execucao()

        def lento():  # a primeira tentativa esgota o prazo de 420 s
            self.relogio.sleep(C.INFRA_DEADLINE_S)
            return RuntimeError("Read timed out")

        feed = Feed(self.relogio, status=[status_xml(self.pub0 + timedelta(minutes=5))], infra=[lento, INFRA])
        resumo = self.correr(feed)
        self.assertEqual(feed.pedidos["infra"], 1, "nenhuma tentativa começa depois do prazo")
        self.assertIn("sem tempo para nova tentativa", resumo)
        self.verificar_estado(2)

    def test_c_execucao_longa_com_espera(self):
        # Estado com a versão publicada há 5 min: a primeira resposta é repetida e a execução
        # espera pela seguinte; o inventário fica para a execução seguinte.
        pub1 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=5)
        self.correr(Feed(self.relogio, status=[status_xml(pub1)], infra=[INFRA]))
        self.forcar_inventario_em_atraso()
        antes = self.static_bytes()
        feed = Feed(self.relogio, status=[status_xml(pub1), status_xml(pub1 + timedelta(minutes=5), a1="charging")],
                    infra=[INFRA])
        resumo = self.correr(feed)
        self.assertIn("Nova versão obtida após a espera", resumo)
        self.assertIn("Inventário adiado", resumo)
        self.assertEqual(feed.pedidos["infra"], 0)
        self.assertEqual(self.static_bytes(), antes)
        am = self.verificar_estado(2)
        self.assertEqual(am[-1]["version"], "nova")
        self.assertNotEqual(am[-1]["wait_s"], "0")

        # Execução seguinte, rápida: o inventário é atualizado.
        feed = Feed(self.relogio, status=[status_xml(pub1 + timedelta(minutes=10))], infra=[INFRA])
        resumo = self.correr(feed)
        self.assertEqual(feed.pedidos["infra"], 1)
        self.assertNotEqual(self.static_bytes()["static_meta.json"], antes["static_meta.json"])
        self.verificar_estado(3)

    def test_c2_execucao_longa_mais_de_180_s(self):
        self.primeira_execucao()
        antes = self.static_bytes()

        def lento():  # o download do estado demora 200 s
            self.relogio.sleep(200)
            return status_xml(self.pub0 + timedelta(minutes=5))

        feed = Feed(self.relogio, status=[lento], infra=[INFRA])
        resumo = self.correr(feed)
        self.assertIn("Inventário adiado", resumo)
        self.assertEqual(feed.pedidos["infra"], 0)
        self.assertEqual(self.static_bytes(), antes)
        self.verificar_estado(2)

    def test_erro_depois_da_amostra_nao_duplica(self):
        self.primeira_execucao()
        original = collect.say

        def say(line=""):
            if "Pontos no feed" in line:
                raise ValueError("erro simulado")
            original(line)

        collect.SAMPLE_WRITTEN = False
        self.proxima_execucao()
        with mock.patch.object(collect, "say", say), \
                mock.patch.object(collect, "fetch", Feed(self.relogio, status=[status_xml(self.pub0 + timedelta(minutes=5))])):
            with self.assertRaises(ValueError):
                collect.main()
        am = self.verificar_estado(2)
        self.assertEqual(am[-1]["ok"], "1")


class TestResumoPublico(Base):
    """O resumo é público: sem contagens por estado, erros só com tipo e mensagem."""

    def test_execucao_normal_sem_estados_com_preenchimento(self):
        self.proxima_execucao()
        resumo = self.correr(Feed(self.relogio, status=[status_xml(self.pub0)], infra=[INFRA]))
        self.assertIn("Preenchimento dos atributos do inventário", resumo)
        self.assertIn("| local |", resumo)
        self.assertIn("| ponto |", resumo)
        self.assertNotIn("Estados no feed", resumo)
        for st in ("`available`", "`charging`", "`outOfOrder`"):
            self.assertNotIn(st, resumo)

    def test_erro_interno_sem_traceback(self):
        self.proxima_execucao()
        with mock.patch.object(collect, "record_tariffs", side_effect=ValueError("teste\nsegunda linha")), \
                mock.patch.object(collect, "fetch", Feed(self.relogio, status=[status_xml(self.pub0)])):
            with self.assertRaises(ValueError):
                collect.main()
        resumo = "\n".join(collect.SUMMARY)
        self.assertIn("> ❌ Erro interno: `ValueError: teste`", resumo)
        self.assertNotIn("Traceback", resumo)
        self.assertNotIn("```", resumo)
        self.assertNotIn("segunda linha", resumo)

    def test_feed_vazio_sem_excerto_xml(self):
        with mock.patch.object(collect, "parse_status", return_value=([], "<excerto/>", None)):
            resumo = self.correr(Feed(self.relogio, status=[status_xml(self.pub0)]))
        self.assertIn("> ❌ Feed vazio ou parcial (0 pontos)", resumo)
        self.assertNotIn("```", resumo)
        self.assertNotIn("<", resumo)

    def test_xml_invalido_so_tipo_e_mensagem(self):
        truncado = status_xml(self.pub0)[:400]
        resumo = self.correr(Feed(self.relogio, status=[truncado, truncado]))
        linhas = [l for l in resumo.splitlines() if l.startswith("> ❌")]
        self.assertEqual(len(linhas), 1)
        self.assertIn("XML inválido", linhas[0])
        self.assertIn("XMLSyntaxError", linhas[0])
        self.assertNotIn("```", resumo)
        for trecho in ("<?xml", "<d2:", "<!--", "datex2.eu"):  # nada do conteúdo da resposta
            self.assertNotIn(trecho, resumo)

    def test_modo_privado_mantem_detalhe(self):
        with mock.patch.object(C, "PUBLIC_SUMMARY", False):
            resumo = self.correr(Feed(self.relogio, status=[status_xml(self.pub0)], infra=[INFRA]))
            self.assertIn("Estados no feed", resumo)
            self.assertIn("`available`", resumo)
            self.proxima_execucao()
            collect.SUMMARY.clear()
            with mock.patch.object(collect, "record_tariffs", side_effect=ValueError("teste")), \
                    mock.patch.object(collect, "fetch",
                                      Feed(self.relogio, status=[status_xml(self.pub0 + timedelta(minutes=5))])):
                with self.assertRaises(ValueError):
                    collect.main()
        self.assertIn("Traceback", "\n".join(collect.SUMMARY))


class TestGravacaoAtomica(Base):
    def test_d_corte_em_append_gz_csv(self):
        p = self.state / "events" / "2026-01-01.csv.gz"
        collect.append_gz_csv(p, ["a", "b"], [("1", "2")])
        antes = p.read_bytes()
        with mock.patch.object(collect.os, "replace", side_effect=OSError("corte simulado")):
            with self.assertRaises(OSError):
                collect.append_gz_csv(p, ["a", "b"], [("3", "4")])
        self.assertEqual(p.read_bytes(), antes)
        self.assertEqual(pd.read_csv(p, dtype=str, compression="gzip").values.tolist(), [["1", "2"]])
        self.assertEqual(list((self.state / "events").iterdir()), [p])
        self.assertEqual(list((self.state / ".tmp").iterdir()), [])
        collect.append_gz_csv(p, ["a", "b"], [("3", "4")])
        self.assertEqual(pd.read_csv(p, dtype=str, compression="gzip").values.tolist(), [["1", "2"], ["3", "4"]])

    def test_d_corte_a_meio_da_escrita(self):
        collect.save_last_status({"X": "available"})
        p = self.state / "last_status.csv.gz"
        antes = p.read_bytes()
        with self.assertRaises(KeyboardInterrupt):
            with collect.atomic_write(p, "gz") as f:
                f.write("point_id,status\n" + "Y,charging\n" * 1000)
                raise KeyboardInterrupt  # corte a meio
        self.assertEqual(p.read_bytes(), antes)
        self.assertEqual(collect.load_last_status(), {"X": "available"})
        self.assertEqual(list((self.state / ".tmp").iterdir()), [])

    def test_d_temporarios_de_execucao_cortada_sao_limpos(self):
        self.primeira_execucao()
        (self.state / ".tmp").mkdir(exist_ok=True)
        (self.state / ".tmp" / "last_status.csv.gz.abc").write_bytes(b"\x1f\x8b parcial")
        self.correr(Feed(self.relogio, status=[status_xml(self.pub0 + timedelta(minutes=5))]))
        self.verificar_estado(2)

    def test_formato_append_compativel(self):
        """O ficheiro acrescentado continua a ser gzip com membros concatenados (lido pelo arquivo)."""
        p = self.state / "samples" / "2026-01-01.csv.gz"
        collect.append_gz_csv(p, ["x"], [("1",)])
        collect.append_gz_csv(p, ["x"], [("2",)])
        with gzip.open(p, "rt", encoding="utf-8") as f:
            self.assertEqual(f.read().split(), ["x", "1", "2"])


if __name__ == "__main__":
    unittest.main()
