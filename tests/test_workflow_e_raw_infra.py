"""Testes do workflow (segredos, repetição do push) e da cópia semanal do raw_infra.

Os blocos `run` são lidos do próprio recolha.yml e corridos com bash, como no Actions.
O repositório de dados é fictício (org/repo-privado); o "remoto" é um repositório git local.

Executar: python -m unittest discover tests -v
"""
import os
import re
import subprocess
import tempfile
import unittest
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from unittest import mock

from coletor import archive
from coletor import collect
from coletor import config as C
from tests.test_robustez import INFRA, Agora, Base, Feed, status_xml

WORKFLOW = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "recolha.yml"
REPO = "org/repo-privado"
TOKEN = "token-ficticio"
URL = f"https://x-access-token:{TOKEN}@github.com/{REPO}.git"


# ---------------------------------------------------------------- leitura do workflow
def indent(linha):
    return len(linha) - len(linha.lstrip(" "))


def blocos_env(texto):
    """Chaves de todos os blocos `env:` do ficheiro (workflow, job e passos)."""
    linhas = texto.splitlines()
    out = []
    for i, l in enumerate(linhas):
        if re.fullmatch(r"\s*(- )?env:\s*", l):
            base = indent(l)
            chaves = {}
            for m in linhas[i + 1:]:
                if m.strip() and indent(m) <= base:
                    break
                if m.strip() and not m.strip().startswith("#"):
                    k, _, v = m.strip().partition(":")
                    chaves[k] = v.strip()
            out.append(chaves)
    return out


def passos(texto):
    """Passos do job: {nome: {"env": {...}, "run": "..."}} (leitura por indentação, sem PyYAML)."""
    linhas = texto.splitlines()
    inicios = [i for i, l in enumerate(linhas) if re.match(r"^      - ", l)]
    out = {}
    for a, b in zip(inicios, inicios[1:] + [len(linhas)]):
        bloco = [l.replace("      - ", "        ", 1) if j == 0 else l
                 for j, l in enumerate(linhas[a:b])]
        nome, env, run = None, {}, []
        i = 0
        while i < len(bloco):
            l = bloco[i]
            if indent(l) == 8 and l.strip().startswith("name:"):
                nome = l.split(":", 1)[1].strip()
            elif indent(l) == 8 and l.strip() == "env:":
                i += 1
                while i < len(bloco) and (not bloco[i].strip() or indent(bloco[i]) > 8):
                    if bloco[i].strip():
                        k, _, v = bloco[i].strip().partition(":")
                        env[k] = v.strip()
                    i += 1
                continue
            elif indent(l) == 8 and l.strip() == "run: |":
                i += 1
                while i < len(bloco) and (not bloco[i].strip() or indent(bloco[i]) > 8):
                    run.append(bloco[i][10:])
                    i += 1
                continue
            i += 1
        if nome:
            out[nome] = {"env": env, "run": "\n".join(run).rstrip() + "\n"}
    return out


TEXTO = WORKFLOW.read_text(encoding="utf-8")
PASSOS = passos(TEXTO)


def correr_passo(nome, cwd, extra_env=None):
    """Corre o bloco `run` de um passo com bash -eo pipefail (como o Actions)."""
    script = Path(cwd) / f".passo_{nome.replace(' ', '_')}.sh"
    script.write_text(PASSOS[nome]["run"], encoding="utf-8")
    env = {"PATH": os.environ["PATH"], "HOME": str(cwd), "LANG": "C.UTF-8",
           "DATA_REPO": REPO, "DATA_TOKEN": TOKEN, **(extra_env or {})}
    r = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", str(script)],
                       cwd=cwd, env=env, capture_output=True, text=True, timeout=60)
    script.unlink()
    return r


# ---------------------------------------------------------------- (c) workflow
class TestWorkflowSegredos(unittest.TestCase):
    def test_sem_vars_data_repo(self):
        self.assertNotIn("vars." + "DATA_REPO", TEXTO)  # montado, para a pesquisa no repositório dar 0
        self.assertNotIn("vars.", TEXTO)

    def test_data_url_fora_dos_blocos_env(self):
        blocos = blocos_env(TEXTO)
        self.assertTrue(blocos)
        for chaves in blocos:
            self.assertNotIn("DATA_URL", chaves)

    def test_passos_com_endereco_usam_segredos(self):
        com_url = [n for n, p in PASSOS.items() if "$DATA_URL" in p["run"]]
        self.assertTrue({"Obter estado", "Dias por arquivar", "Exportar para branch data",
                         "Guardar estado"} <= set(com_url))
        for n in com_url:
            self.assertEqual(PASSOS[n]["env"], {"DATA_REPO": "${{ secrets.DATA_REPO }}",
                                                "DATA_TOKEN": "${{ secrets.DATA_TOKEN }}"}, n)
            self.assertIn('DATA_URL="https://x-access-token:${DATA_TOKEN}@github.com/${DATA_REPO}.git"',
                          PASSOS[n]["run"], n)
        for n, p in PASSOS.items():  # os outros passos não recebem os segredos
            if n not in com_url:
                self.assertNotIn("DATA_REPO", p["env"], n)
                self.assertNotIn("DATA_TOKEN", p["env"], n)


# ---------------------------------------------------------------- (a) repetição do push
GIT_STUB = """#!/usr/bin/env bash
echo "$*" >> "$REG/git.log"
if [ "$1" = push ]; then
  n=$(cat "$REG/falhas")
  if [ "$n" -gt 0 ]; then
    echo $((n - 1)) > "$REG/falhas"
    echo "fatal: falha simulada" >&2
    exit 1
  fi
fi
exit 0
"""
SLEEP_STUB = """#!/usr/bin/env bash
echo "$1" >> "$REG/sleep.log"
"""


class TestPushEstado(unittest.TestCase):
    def guardar(self, falhas):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "state").mkdir()
            (d / "bin").mkdir()
            for nome, txt in (("git", GIT_STUB), ("sleep", SLEEP_STUB)):
                (d / "bin" / nome).write_text(txt)
                (d / "bin" / nome).chmod(0o755)
            (d / "falhas").write_text(str(falhas))
            r = correr_passo("Guardar estado", d, {"PATH": f"{d / 'bin'}:{os.environ['PATH']}",
                                                   "REG": str(d)})
            git = (d / "git.log").read_text().splitlines()
            esperas = [int(x) for x in (d / "sleep.log").read_text().split()] \
                if (d / "sleep.log").exists() else []
        out = r.stdout + r.stderr
        self.assertNotIn(REPO, out)
        self.assertNotIn(TOKEN, out)
        return r, [l for l in git if l.startswith("push")], esperas, out

    def test_sucesso_a_terceira_tentativa(self):
        r, pushes, esperas, out = self.guardar(falhas=2)
        self.assertEqual(r.returncode, 0, out)
        self.assertEqual(len(pushes), 3)
        self.assertEqual(esperas, [10, 20])
        self.assertIn("Estado guardado.", r.stdout)  # o código depois do ciclo corre
        self.assertNotIn("::error::", out)

    def test_quatro_falhas_falha_o_passo(self):
        r, pushes, esperas, out = self.guardar(falhas=4)
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(len(pushes), 4)
        self.assertEqual(esperas, [10, 20, 40])
        self.assertIn("::error::Push do estado falhou", out)
        self.assertNotIn("Estado guardado.", r.stdout)

    def test_sem_falhas_sem_esperas(self):
        r, pushes, esperas, _ = self.guardar(falhas=0)
        self.assertEqual(r.returncode, 0)
        self.assertEqual((len(pushes), esperas), (1, []))


# ---------------------------------------------------------------- (b) raw_infra semanal
def dia(semana, dia_semana, hora=10):
    return datetime.combine(date.fromisocalendar(2026, semana, dia_semana), time(hora), timezone.utc)


class TestRawInfraSemanal(Base):
    def setUp(self):
        super().setUp()
        self.data = Path(self.tmp.name) / "data"
        self.n_status = 0
        for p in (mock.patch.object(C, "DATA_DIR", self.data),
                  mock.patch.object(archive, "MANIFEST", self.state / ".archive_manifest.json"),
                  mock.patch.object(archive, "MANIFEST_HASH", self.state / ".static_hash_pending.txt")):
            p.start()
            self.patches.append(p)

    def execucao_em(self, quando):
        """Uma execução com inventário em atraso, à hora `quando`."""
        Agora.atual = quando - timedelta(minutes=5)  # correr() avança 5 min
        meta = self.state / "static_meta.json"
        if meta.exists():
            old = (quando - timedelta(hours=C.STATIC_REFRESH_H + 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            collect.save_json(meta, {**collect.load_json(meta, {}), "ts_utc": old})
        self.n_status += 1
        feed = Feed(self.relogio, status=[status_xml(self.pub0 + timedelta(minutes=5 * self.n_status))],
                    infra=[INFRA])
        self.correr(feed)
        self.assertEqual(feed.pedidos["infra"], 1, "o inventário foi atualizado")

    def arquivar(self):
        archive.export()
        archive.cleanup()

    def copias(self):
        d = self.state / "raw_infra"
        return sorted(p.name for p in d.iterdir()) if d.exists() else []

    def marcador(self):
        return (self.state / collect.RAW_INFRA_MARCADOR).read_text().strip()

    def test_uma_copia_por_semana(self):
        self.execucao_em(dia(40, 1))
        self.assertEqual(self.copias(), ["2026-09-28.W40.infra.xml.gz"])
        self.assertEqual(self.marcador(), "2026-W40")

        self.arquivar()
        self.assertEqual(self.copias(), [], "a cópia passou para o arquivo")
        self.assertTrue((self.data / "2026" / "raw_infra" / "2026-09-28.W40.infra.xml.gz").exists())
        self.assertEqual(self.marcador(), "2026-W40", "o arquivo diário não apaga o marcador")

        self.execucao_em(dia(40, 2))  # dia seguinte, mesma semana ISO
        self.assertEqual(self.copias(), [], "uma só cópia por semana")
        self.arquivar()
        self.assertEqual(sorted(p.name for p in (self.data / "2026" / "raw_infra").iterdir()),
                         ["2026-09-28.W40.infra.xml.gz"])

        self.execucao_em(dia(41, 1))  # semana seguinte
        self.assertEqual(self.copias(), ["2026-10-05.W41.infra.xml.gz"])
        self.assertEqual(self.marcador(), "2026-W41")
        self.verificar_estado(1)  # as amostras dos dias arquivados já saíram do estado

    def test_semana_em_hora_de_lisboa(self):
        # Domingo 23:30 UTC = segunda 00:30 em Lisboa (hora de verão): já é a semana seguinte.
        self.execucao_em(datetime(2026, 9, 27, 23, 30, tzinfo=timezone.utc))
        self.assertEqual(self.marcador(), "2026-W40")
        self.assertEqual(self.copias(), ["2026-09-28.W40.infra.xml.gz"])

    def test_transicao_sem_marcador_com_copia_da_semana(self):
        self.execucao_em(dia(40, 1))
        (self.state / collect.RAW_INFRA_MARCADOR).unlink()  # estado anterior a esta versão
        self.execucao_em(dia(40, 1, hora=16))
        self.assertEqual(self.copias(), ["2026-09-28.W40.infra.xml.gz"])
        self.assertEqual(self.marcador(), "2026-W40")

    def test_marcador_sobrevive_a_guardar_e_obter(self):
        base = Path(self.tmp.name)
        remoto = base / "remoto.git"
        subprocess.run(["git", "init", "-q", "--bare", str(remoto)], check=True)
        gitconfig = base / "gitconfig"
        gitconfig.write_text(f'[user]\n\tname = teste\n\temail = teste@example.invalid\n'
                             f'[url "{remoto}"]\n\tinsteadOf = {URL}\n')
        env = {"GIT_CONFIG_GLOBAL": str(gitconfig), "GIT_CONFIG_NOSYSTEM": "1"}

        r = correr_passo("Obter estado", base, env)  # branch state inexistente: estado novo
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertTrue((self.state / ".novo").exists())
        self.execucao_em(dia(40, 1))
        r = correr_passo("Guardar estado", base, env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("Estado guardado.", r.stdout)

        outra = base / "execucao_seguinte"
        outra.mkdir()
        r = correr_passo("Obter estado", outra, env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("Estado lido", r.stdout)
        self.assertEqual((outra / "state" / collect.RAW_INFRA_MARCADOR).read_text().strip(), "2026-W40")
        self.assertTrue((outra / "state" / "raw_infra" / "2026-09-28.W40.infra.xml.gz").exists())
        self.assertNotIn(TOKEN, r.stdout + r.stderr)

    def test_obter_estado_sem_segredo_falha_sem_criar_estado(self):
        r = correr_passo("Obter estado", self.tmp.name, {"DATA_REPO": ""})
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("::error::Segredo DATA_REPO em falta", r.stdout)
        self.assertFalse(self.state.exists())
        r = correr_passo("Guardar estado", self.tmp.name, {"DATA_REPO": ""})
        self.assertEqual(r.returncode, 0)
        self.assertIn("Sem estado para guardar.", r.stdout)


if __name__ == "__main__":
    unittest.main()
