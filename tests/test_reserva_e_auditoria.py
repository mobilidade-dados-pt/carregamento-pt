"""Testes da reserva separada e das correções da auditoria.

- recolha.yml sem schedule, com a entrada "origem" e actions fixadas por SHA;
- reserva.yml: cron, condição dos 360 s e disparo forçado (gh fictício);
- recolha ignorada pelo intervalo mínimo: sem push do estado nem arquivo;
- prazo total da descarga do feed com respostas lentas simuladas (relógio simulado);
- arquivo tardio: corre o passo "Exportar para branch data" do workflow contra um repositório
  git local, com o mesmo clone esparso, e confirma o resultado no commit;
- token fora da configuração do git do estado.

Sem rede nem dados reais. Executar: python -m unittest discover tests -v
"""
import csv
import gzip
import io
import os
import re
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import requests

from coletor import collect
from coletor import config as C
from tests.test_robustez import MIN_INTERVAL_REAL, Base, Feed, INFRA, status_xml
from tests.test_workflow_e_raw_infra import (GIT_STUB, PASSOS, REPO, TEXTO, TOKEN, URL, correr_passo,
                                              passos)

RAIZ = Path(__file__).resolve().parent.parent
RESERVA_TXT = (RAIZ / ".github" / "workflows" / "reserva.yml").read_text(encoding="utf-8")
RESERVA = passos(RESERVA_TXT)


def bloco(texto, chave):
    """Linhas do bloco de topo `chave:` (até à chave de topo seguinte)."""
    linhas = texto.splitlines()
    i = linhas.index(f"{chave}:")
    out = []
    for l in linhas[i + 1:]:
        if l and not l.startswith((" ", "#")):
            break
        out.append(l)
    return "\n".join(out)


def stub(pasta, nome, texto):
    p = Path(pasta) / nome
    p.write_text(texto, encoding="utf-8", newline="\n")
    p.chmod(0o755)


# ---------------------------------------------------------------- recolha.yml
class TestRecolhaYml(unittest.TestCase):
    def test_sem_schedule(self):
        gatilhos = bloco(TEXTO, "on")
        self.assertNotIn("schedule", gatilhos)
        self.assertNotIn("cron", TEXTO)
        self.assertIn("workflow_dispatch:", gatilhos)

    def test_origem_externo_por_omissao(self):
        gatilhos = bloco(TEXTO, "on")
        m = re.search(r"\n      origem:\n((?:        .*\n?)+)", gatilhos)
        self.assertIsNotNone(m, "entrada origem")
        campos = dict(l.strip().split(": ", 1) for l in m.group(1).splitlines())
        self.assertEqual(campos["type"], "string")
        self.assertEqual(campos["required"], "false")
        self.assertEqual(campos["default"], "externo")
        self.assertIn("run-name: Recolha · ${{ inputs.origem || 'externo' }}", TEXTO.splitlines())

    def test_actions_fixadas_por_sha(self):
        usos = re.findall(r"uses:\s*(\S+)", TEXTO)
        self.assertEqual(len(usos), 2)
        for u in usos:
            self.assertRegex(u, r"^actions/[\w-]+@[0-9a-f]{40}$")

    def test_versoes_registadas_no_log(self):
        self.assertIn("pip install -q -r requirements.txt && pip freeze", TEXTO)

    def test_requirements_com_versoes_exatas(self):
        linhas = [l.strip() for l in (RAIZ / "requirements.txt").read_text().splitlines()
                  if l.strip() and not l.startswith("#")]
        self.assertTrue(linhas)
        for l in linhas:
            self.assertRegex(l, r"^[A-Za-z0-9_.\-\[\]]+==[0-9][\w.+!-]*$")
        nomes = {l.split("==")[0].lower() for l in linhas}
        self.assertTrue({"requests", "lxml", "pandas", "pyarrow"} <= nomes)

    def test_recolha_ignorada_sem_push_nem_arquivo(self):
        self.assertIn("        id: recolher\n", TEXTO)
        cond = "steps.recolher.outputs.ignorada != '1'"
        self.assertEqual(PASSOS["Guardar estado"]["if"], "${{ !cancelled() && " + cond + " }}")
        for n in ("Dias por arquivar", "Exportar para branch data", "Limpar estado arquivado"):
            self.assertTrue(PASSOS[n]["if"].startswith(cond), n)
        self.assertEqual(PASSOS["Ping healthchecks"]["if"], "success()")  # como antes


# ---------------------------------------------------------------- reserva.yml
GH_STUB = """#!/usr/bin/env bash
echo "$*" >> "$REG/gh.log"
if [ "$1 $2" = "run list" ]; then
  cat "$REG/criada"
  exit "$(cat "$REG/rc")"
fi
exit 0
"""


class TestReserva(unittest.TestCase):
    PASSO = "Disparar a recolha se estiver parada"

    def test_configuracao(self):
        gatilhos = bloco(RESERVA_TXT, "on")
        self.assertIn('    - cron: "2-59/5 * * * *"', gatilhos)
        m = re.search(r"\n      forcar:\n((?:        .*\n?)+)", gatilhos)
        campos = dict(l.strip().split(": ", 1) for l in m.group(1).splitlines())
        self.assertEqual((campos["type"], campos["default"]), ("boolean", "false"))
        self.assertEqual(bloco(RESERVA_TXT, "permissions").strip(), "actions: write")
        self.assertEqual([l.strip() for l in bloco(RESERVA_TXT, "concurrency").splitlines()],
                         ["group: reserva", "cancel-in-progress: false"])
        self.assertIn("    timeout-minutes: 2\n", RESERVA_TXT)
        self.assertNotIn("uses:", RESERVA_TXT)
        self.assertEqual(list(RESERVA), [self.PASSO])
        self.assertNotIn("secrets.", RESERVA_TXT)

    def correr(self, idade=None, rc=0, forcar="false", criada=None):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "bin").mkdir()
            stub(d / "bin", "gh", GH_STUB)
            if criada is None and idade is not None:
                criada = (datetime.now(timezone.utc) - timedelta(seconds=idade)).strftime("%Y-%m-%dT%H:%M:%SZ")
            (d / "criada").write_text(criada or "")
            (d / "rc").write_text(str(rc))
            r = correr_passo(self.PASSO, d, {"PATH": f"{d / 'bin'}:{os.environ['PATH']}", "REG": str(d),
                                             "FORCAR": forcar}, workflow=RESERVA)
            log = (d / "gh.log").read_text().splitlines() if (d / "gh.log").exists() else []
        disparos = [l for l in log if l.startswith("workflow run")]
        return r, disparos, r.stdout + r.stderr

    def assertDisparou(self, disparos):
        self.assertEqual(disparos, ["workflow run recolha.yml --ref main -f origem=reserva"])

    def test_recente_nao_dispara(self):
        r, disparos, out = self.correr(idade=300)
        self.assertEqual(r.returncode, 0, out)
        self.assertEqual(disparos, [])
        self.assertIn("nada a fazer", out)

    def test_mais_de_360_s_dispara(self):
        for idade in (365, 400, 3600):
            r, disparos, out = self.correr(idade=idade)
            self.assertEqual(r.returncode, 0, out)
            self.assertDisparou(disparos)
            self.assertIn("mais de 360 s", out)

    def test_forcar_dispara_com_execucao_recente(self):
        r, disparos, out = self.correr(idade=30, forcar="true")
        self.assertEqual(r.returncode, 0, out)
        self.assertDisparou(disparos)
        self.assertIn("Disparo forçado", out)

    def test_sem_execucoes_ou_erro_de_leitura_dispara_com_aviso(self):
        for kw in ({"criada": ""}, {"idade": 30, "rc": 1}, {"criada": "lixo"}):
            r, disparos, out = self.correr(**kw)
            self.assertEqual(r.returncode, 0, out)
            self.assertDisparou(disparos)
            self.assertIn("::warning::", out)


# ---------------------------------------------------------------- recolha ignorada
class TestRecolhaIgnorada(Base):
    def setUp(self):
        super().setUp()
        p = mock.patch.object(C, "MIN_INTERVAL_S", MIN_INTERVAL_REAL)
        p.start()
        self.patches.append(p)
        self.saida = Path(self.tmp.name) / "github_output"
        p = mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(self.saida)})
        p.start()
        self.patches.append(p)

    def ler_saida(self):
        txt = self.saida.read_text(encoding="utf-8")
        self.saida.unlink()
        return txt

    def test_saida_ignorada(self):
        self.correr(Feed(self.relogio, status=[status_xml(self.pub0)], infra=[INFRA]))
        self.assertEqual(self.ler_saida(), "ignorada=0\n")
        t1 = collect.datetime.atual

        for segundos, esperado in ((60, "ignorada=1\n"), (100, "ignorada=0\n")):
            collect.datetime.atual = t1 + timedelta(seconds=segundos)
            collect.SAMPLE_WRITTEN = False
            collect.SUMMARY.clear()
            antes = sorted((p.relative_to(self.state), p.read_bytes()) for p in self.state.rglob("*")
                           if p.is_file())
            with mock.patch.object(collect, "fetch",
                                   Feed(self.relogio, status=[status_xml(self.pub0 + timedelta(minutes=5))])):
                self.assertEqual(collect.main(), 0)
            self.assertEqual(self.ler_saida(), esperado)
            depois = sorted((p.relative_to(self.state), p.read_bytes()) for p in self.state.rglob("*")
                            if p.is_file())
            if esperado == "ignorada=1\n":
                self.assertEqual(depois, antes, "uma recolha ignorada não altera o estado")
                self.assertIn("ignorada", "\n".join(collect.SUMMARY))
        self.verificar_estado(2)


# ---------------------------------------------------------------- prazo da descarga do feed
class RespostaLenta:
    """Resposta HTTP simulada: cada bloco demora `bloco_s` no relógio simulado."""

    def __init__(self, relogio, bloco_s, blocos=None, corpo=b"x" * 10):
        self.relogio, self.bloco_s, self.blocos, self.corpo = relogio, bloco_s, blocos, corpo
        self.status_code, self.headers = 200, {"ETag": '"e1"'}

    def raise_for_status(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, n):
        i = 0
        while self.blocos is None or i < self.blocos:
            self.relogio.sleep(self.bloco_s)
            yield self.corpo
            i += 1


class Servidor:
    """Substitui requests.get; regista o instante e os timeouts de cada pedido."""

    def __init__(self, relogio, comportamento):
        self.relogio, self.comportamento, self.pedidos = relogio, comportamento, []

    def __call__(self, url, timeout=None, headers=None, stream=False):
        self.pedidos.append((self.relogio.time(), timeout))
        return self.comportamento(timeout)


class TestPrazoFeed(Base):
    def descarregar(self, comportamento):
        srv = Servidor(self.relogio, comportamento)
        t = self.relogio.time()
        prazo = t + C.FEED_DEADLINE_S
        with mock.patch.object(collect.requests, "get", srv):
            with self.assertRaises(collect.FeedError) as cm:
                collect.fetch_parse_status(deadline=prazo)
        for inicio, timeout in srv.pedidos:
            self.assertLess(inicio, prazo, "nenhum pedido começa depois do prazo")
            self.assertLessEqual(inicio + max(timeout), prazo + 1e-9, "timeouts limitados ao restante")
        return self.relogio.time() - t, srv, str(cm.exception)

    def test_resposta_que_nunca_acaba(self):
        dur, srv, erro = self.descarregar(lambda timeout: RespostaLenta(self.relogio, bloco_s=30))
        self.assertLessEqual(dur, C.FEED_DEADLINE_S + 30, "prazo mais um bloco, no máximo")
        self.assertEqual(len(srv.pedidos), 1)
        self.assertIn("prazo", erro)

    def test_servidor_sem_resposta(self):
        def sem_resposta(timeout):
            self.relogio.sleep(timeout[1])
            raise requests.exceptions.ReadTimeout("Read timed out")

        dur, srv, erro = self.descarregar(sem_resposta)
        self.assertLessEqual(dur, C.FEED_DEADLINE_S)
        self.assertEqual(len(srv.pedidos), 2)
        self.assertIn("prazo esgotado", erro)

    def test_tres_respostas_lentas_com_erro(self):
        def lenta(timeout):
            self.relogio.sleep(70)
            raise requests.exceptions.ConnectionError("ligação cortada")

        dur, srv, erro = self.descarregar(lenta)
        self.assertLessEqual(dur, C.FEED_DEADLINE_S)
        self.assertEqual(len(srv.pedidos), 3)

    def test_resposta_normal_pelo_caminho_real(self):
        corpo = status_xml(self.pub0)
        srv = Servidor(self.relogio, lambda timeout: RespostaLenta(self.relogio, 0.1, blocos=1, corpo=corpo))
        with mock.patch.object(collect.requests, "get", srv):
            res = collect.fetch_parse_status(deadline=self.relogio.time() + C.FEED_DEADLINE_S)
        self.assertEqual(res["raw"], corpo)
        self.assertEqual(len(res["rows"]), 4)
        self.assertEqual(res["hdr"]["h_etag"], '"e1"')

    def test_execucao_completa_regista_falha_dentro_do_prazo(self):
        srv = Servidor(self.relogio, lambda timeout: RespostaLenta(self.relogio, bloco_s=30))
        self.proxima_execucao()
        with mock.patch.object(collect.requests, "get", srv):
            self.assertEqual(collect.main(), 0)
        am = self.verificar_estado(1)
        self.assertEqual(am[0]["version"], "falha")
        self.assertIn("prazo", am[0]["err"])
        self.assertLessEqual(float(am[0]["secs"]), C.FEED_DEADLINE_S + 30)


# ---------------------------------------------------------------- arquivo tardio (passo real)
def git(*args, cwd=None, env=None):
    r = subprocess.run(["git", *args], cwd=cwd, env=env, capture_output=True, text=True, encoding="utf-8")
    if r.returncode != 0:
        raise AssertionError(f"git {' '.join(args)}: {r.stderr}")
    return r.stdout


SAMPLE_COLS = collect.SAMPLE_COLS


class TestArquivoTardio(unittest.TestCase):
    DIA = "2026-11-01"
    PASSO = "Exportar para branch data"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.remoto = self.base / "remoto.git"
        git("init", "-q", "--bare", "-b", "data", str(self.remoto))
        git("-C", str(self.remoto), "config", "uploadpack.allowFilter", "true")
        self.gitconfig = self.base / "gitconfig"
        self.gitconfig.write_text(
            "[user]\n\tname = teste\n\temail = teste@example.invalid\n"
            f'[url "{self.remoto.as_uri()}"]\n\tinsteadOf = {URL}\n'
            "[uploadpack]\n\tallowFilter = true\n[protocol \"file\"]\n\tallow = always\n",
            encoding="utf-8")
        self.genv = {**os.environ, "GIT_CONFIG_GLOBAL": str(self.gitconfig), "GIT_CONFIG_NOSYSTEM": "1"}
        # branch data inicial, só com um ficheiro na raiz
        ini = self.base / "ini"
        git("init", "-q", "-b", "data", str(ini), env=self.genv)
        (ini / "LEIA-ME.txt").write_text("arquivo\n")
        git("-C", str(ini), "add", "-A", env=self.genv)
        git("-C", str(ini), "commit", "-qm", "inicio", env=self.genv)
        git("-C", str(ini), "push", "-q", str(self.remoto), "data", env=self.genv)
        self.state = self.base / "state"
        self.bin = self.base / "bin"
        self.bin.mkdir()
        # "python" do passo = o interpretador dos testes (com as dependências)
        stub(self.bin, "python", f'#!/usr/bin/env bash\nexec "{Path(sys.executable).as_posix()}" "$@"\n')
        self.n = 0

    def tearDown(self):
        self.tmp.cleanup()

    def estado(self, linhas, raw=None):
        d = self.state / "samples"
        d.mkdir(parents=True, exist_ok=True)
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(SAMPLE_COLS)
        for i in range(linhas):
            w.writerow([f"{self.DIA}T10:{i:02d}:00Z", 1, 200, "nova"] + [""] * (len(SAMPLE_COLS) - 4))
        (d / f"{self.DIA}.csv.gz").write_bytes(gzip.compress(buf.getvalue().encode(), mtime=0))
        if raw is not None:
            (self.state / "raw").mkdir(exist_ok=True)
            (self.state / "raw" / f"{self.DIA}.status.xml.gz").write_bytes(gzip.compress(raw, mtime=0))

    def exportar(self):
        """Corre o passo do workflow (clone esparso, export, add --sparse, commit, push)."""
        self.n += 1
        cwd = self.base / f"exec{self.n}"
        cwd.mkdir()
        resumo = cwd / "resumo.md"
        r = correr_passo(self.PASSO, cwd, {
            "PATH": f"{self.bin.as_posix()}:{os.environ['PATH']}",
            "GIT_CONFIG_GLOBAL": str(self.gitconfig), "GIT_CONFIG_NOSYSTEM": "1",
            "STATE_DIR": str(self.state), "DATA_DIR": str(cwd / "data"),
            "PYTHONPATH": str(RAIZ), "PYTHONIOENCODING": "utf-8",
            "GITHUB_STEP_SUMMARY": str(resumo), "TZ": "UTC"})
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        self.assertNotIn(TOKEN, out)
        self.assertNotIn(REPO, out)
        cfg = (cwd / "data" / ".git" / "config").read_text(encoding="utf-8")
        self.assertNotIn(TOKEN, cfg, "o endereço com o token sai da configuração no fim do passo")
        self.assertEqual(git("-C", str(cwd / "data"), "config", "core.sparseCheckout", env=self.genv).strip(),
                         "true", "o clone é esparso, como no Actions")
        self.ultimo = cwd
        # limpeza do estado, como o passo "Limpar estado arquivado"
        subprocess.run([sys.executable, "-m", "coletor.archive", "cleanup"], cwd=RAIZ, check=True,
                       env={**os.environ, "STATE_DIR": str(self.state)}, capture_output=True)
        return out, resumo.read_text(encoding="utf-8") if resumo.exists() else ""

    def arvore(self, pasta):
        """{caminho: hash do blob} no commit do branch data do remoto."""
        out = git("-C", str(self.remoto), "ls-tree", "-r", "data", "--", pasta)
        return {l.split("\t")[1]: l.split()[2] for l in out.splitlines()}

    def test_tardio_vai_para_pasta_propria_e_nao_substitui_o_dia(self):
        self.estado(1, raw=b"<xml>1</xml>")
        out, resumo = self.exportar()
        dia = self.arvore("2026")
        self.assertEqual(set(dia), {f"2026/samples/{self.DIA}.parquet", f"2026/raw/{self.DIA}.status.xml.gz"})
        self.assertEqual(self.arvore("_tardios"), {})
        self.assertNotIn("::warning::", out)

        # Dados tardios do mesmo dia (conteúdo diferente).
        self.estado(2, raw=b"<xml>2</xml>")
        out, resumo = self.exportar()
        self.assertFalse((self.ultimo / "data" / "2026" / "samples" / f"{self.DIA}.parquet").exists(),
                         "o ficheiro já arquivado não está no disco (clone esparso, sem blobs)")
        self.assertEqual(self.arvore("2026"), dia, "as pastas do dia ficam com os mesmos ficheiros e bytes")
        self.assertEqual(set(self.arvore("_tardios")),
                         {f"_tardios/2026/samples/{self.DIA}.tardio-1.parquet",
                          f"_tardios/2026/raw/{self.DIA}.tardio-1.status.xml.gz"})
        self.assertIn(f"::warning::Arquivo: ficheiro tardio de samples {self.DIA} gravado como "
                      f"_tardios/2026/samples/{self.DIA}.tardio-1.parquet", out)
        self.assertIn("o dia arquivado não foi substituído", resumo)

        # O mesmo conteúdo outra vez (ex.: push do estado falhado): não grava nada.
        tard = self.arvore("_tardios")
        self.estado(2, raw=b"<xml>2</xml>")
        out, resumo = self.exportar()
        self.assertEqual(self.arvore("_tardios"), tard)
        self.assertEqual(self.arvore("2026"), dia)
        self.assertNotIn("::warning::", out)

        # Outro conteúdo: tardio-2.
        self.estado(3)
        out, _ = self.exportar()
        self.assertEqual(self.arvore("2026"), dia)
        self.assertIn(f"_tardios/2026/samples/{self.DIA}.tardio-2.parquet", self.arvore("_tardios"))

    def test_mesmo_conteudo_do_dia_nao_gera_tardio(self):
        self.estado(1)
        self.exportar()
        dia = self.arvore("2026")
        self.estado(1)
        out, resumo = self.exportar()
        self.assertEqual(self.arvore("2026"), dia)
        self.assertEqual(self.arvore("_tardios"), {})
        self.assertNotIn("::warning::", out)
        self.assertEqual(resumo, "")


# ---------------------------------------------------------------- token fora da configuração do git
PYTHON_STUB = """#!/usr/bin/env bash
echo "$*" >> "$REG/python.log"
"""


class TestTokenForaDaConfiguracao(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        remoto = self.base / "remoto.git"
        git("init", "-q", "--bare", str(remoto))
        gitconfig = self.base / "gitconfig"
        gitconfig.write_text(f'[user]\n\tname = teste\n\temail = teste@example.invalid\n'
                             f'[url "{remoto.as_posix()}"]\n\tinsteadOf = {URL}\n', encoding="utf-8")
        self.env = {"GIT_CONFIG_GLOBAL": str(gitconfig), "GIT_CONFIG_NOSYSTEM": "1"}

    def tearDown(self):
        self.tmp.cleanup()

    def obter_e_guardar(self):
        a = self.base / "a"
        a.mkdir()
        r = correr_passo("Obter estado", a, self.env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        (a / "state" / "x.txt").write_text("1")
        r = correr_passo("Guardar estado", a, self.env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("Estado guardado.", r.stdout)
        b = self.base / "b"
        b.mkdir()
        r = correr_passo("Obter estado", b, self.env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("Estado lido", r.stdout)
        return b

    def recolher(self, cwd):
        (cwd / "bin").mkdir(exist_ok=True)
        stub(cwd / "bin", "python", PYTHON_STUB)
        r = correr_passo("Recolher", cwd, {"PATH": f"{cwd / 'bin'}:{os.environ['PATH']}", "REG": str(cwd),
                                           "DATA_REPO": "", "DATA_TOKEN": ""})
        corrida = (cwd / "python.log").read_text().strip() if (cwd / "python.log").exists() else ""
        return r, corrida

    def test_estado_sem_token_e_recolha_corre(self):
        b = self.obter_e_guardar()
        cfg = (b / "state" / ".git" / "config").read_text(encoding="utf-8")
        self.assertNotIn(TOKEN, cfg)
        self.assertNotIn("x-access-token", cfg)
        self.assertNotIn("[remote", cfg)
        self.assertEqual((b / "state" / "x.txt").read_text(), "1")
        r, corrida = self.recolher(b)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(corrida, "-m coletor.collect")
        self.assertNotIn("::error::", r.stdout + r.stderr)

    def test_token_na_configuracao_falha_sem_perder_a_amostra(self):
        b = self.obter_e_guardar()
        git("-C", str(b / "state"), "remote", "add", "origin", URL)
        r, corrida = self.recolher(b)
        out = r.stdout + r.stderr
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(corrida, "-m coletor.collect", "a recolha corre na mesma")
        self.assertIn("::error::Token encontrado na configuração do git do estado.", out)
        self.assertNotIn(TOKEN, out)

    def test_push_do_estado_para_o_endereco_do_passo(self):
        d = self.base
        (d / "state").mkdir()
        stub(self.bin_dir(), "git", GIT_STUB)
        (d / "falhas").write_text("0")
        r = correr_passo("Guardar estado", d, {"PATH": f"{self.bin_dir()}:{os.environ['PATH']}", "REG": str(d)})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        pushes = [l for l in (d / "git.log").read_text().splitlines() if l.startswith("push")]
        self.assertEqual(pushes, [f"push -qf {URL} snap:state"])

    def bin_dir(self):
        p = self.base / "bin"
        p.mkdir(exist_ok=True)
        return p


if __name__ == "__main__":
    unittest.main()
