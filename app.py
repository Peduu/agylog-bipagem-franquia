from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from uuid import uuid4


APP_DIR = Path(__file__).resolve().parent
LOCAL_STORAGE_ROOT = Path("/root/franquia-data")
DATA_DIR = LOCAL_STORAGE_ROOT / "data"
DB_PATH = DATA_DIR / "pedidos.db"
TOOLS_DIR = APP_DIR / "tools"
BUNDLED_PYTHON = Path("python3")

ORDER_COLUMN = "Nro. Pedido"
FRANCHISE_COLUMN = "Sigla Unidade Entrega"
FALLBACK_FRANCHISE_COLUMN = "Sigla Unidade Atual"

TMS_BASE_URL = "https://corellilog.tmselite.com"
TMS_CLIENT_IDS = "3994,3960,3969,3997,4023,3966,3995,4000,3965,3998,4018,3955,3970,4038,4002,4008,4013,4020,3974,3952,3968,3972,3962,3963,4009,3999,3975,4005,4006,3964,3996,3971,4010,3957,3967,4003,3973"

_sync_lock = threading.Lock()
_sync_state: dict = {"status": "idle", "message": "Nenhuma sincronização realizada.", "updated": None}


def now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_tms_credentials() -> tuple[str, str]:
    env_path = DATA_DIR.parent / ".env"
    creds: dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                creds[k.strip()] = v.strip()
    login = creds.get("TMS_LOGIN") or os.environ.get("TMS_LOGIN", "")
    senha = creds.get("TMS_SENHA") or os.environ.get("TMS_SENHA", "")
    if not login or not senha:
        raise RuntimeError("Credenciais TMS não configuradas. Crie o arquivo .env com TMS_LOGIN e TMS_SENHA.")
    return login, senha


def _tms_login(session, login: str, senha: str) -> None:
    base_host = urlparse(TMS_BASE_URL).hostname or ""

    session.get(f"{TMS_BASE_URL}/login", timeout=30)
    resp = session.post(
        f"{TMS_BASE_URL}/login",
        data={
            "login": login,
            "senha": senha,
            "returnUrl": "",
            "logUsuariosistemaAcesso.latitude": "",
            "logUsuariosistemaAcesso.longitude": "",
        },
        headers={"X-Requested-With": "XMLHttpRequest"},
        timeout=30,
    )
    resp.raise_for_status()

    content_type = resp.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = resp.json()
        if payload.get("error") not in (False, None):
            raise RuntimeError("Login no TMS falhou — verifique as credenciais.")

        minutos = payload.get("minutosInatividade")
        if minutos:
            session.cookies.set("nsfynanifij", str(minutos), domain=base_host, path="/")
        return

    if 'id="formLogin"' in resp.text or "Acesso restrito" in resp.text:
        raise RuntimeError("Login no TMS falhou — verifique as credenciais.")

    if "nsfynanifij" not in session.cookies and base_host:
        session.cookies.set("nsfynanifij", "30", domain=base_host, path="/")


def _download_tms_report(session, days: int) -> tuple[bytes, str]:
    dt_fim = datetime.now()
    dt_ini = dt_fim - timedelta(days=days)
    fmt = "%d/%m/%Y"

    ids_list = [i.strip() for i in TMS_CLIENT_IDS.split(",")]
    form_data: list[tuple[str, str]] = [
        ("dtIniSolicitacao", dt_ini.strftime(fmt)),
        ("dtFimSolicitacao", dt_fim.strftime(fmt)),
        ("flagApresentarVolumes", "N"),
    ]
    for cid in ids_list:
        form_data.append(("idsCliente", cid))

    resp = session.post(
        f"{TMS_BASE_URL}/EntregasRelatorios/RelatorioGeralEntregas",
        data=form_data,
        timeout=60,
    )
    resp.raise_for_status()
    if 'id="formLogin"' in resp.text or "Acesso restrito" in resp.text:
        raise RuntimeError("Sessão do TMS expirou antes da geração do relatório.")

    legacy_match = re.search(r'id="btDownload"[^>]+href="([^"]+)"', resp.text)
    if legacy_match:
        legacy_path = legacy_match.group(1)
        if legacy_path and not legacy_path.startswith("javascript:"):
            legacy_resp = session.get(f"{TMS_BASE_URL}{legacy_path}", timeout=120)
            legacy_resp.raise_for_status()
            legacy_name = legacy_path.split("nomeArquivo=")[-1] if "nomeArquivo=" in legacy_path else "tms_sync.csv"
            return legacy_resp.content, legacy_name

    generate_resp = session.get(
        f"{TMS_BASE_URL}/EntregasRelatorios/RelatorioGeralEntregas/gerar-excel",
        timeout=60,
    )
    generate_resp.raise_for_status()
    try:
        generate_payload = generate_resp.json()
    except ValueError as exc:
        raise RuntimeError("Resposta inesperada ao gerar relatório no TMS.") from exc

    filename = clean_cell(generate_payload.get("nomeArquivo"))
    if not filename:
        raise RuntimeError("TMS não retornou o nome do arquivo para download.")

    download_resp = session.get(
        f"{TMS_BASE_URL}/EntregasRelatorios/RelatorioGeralEntregas/download-excel",
        params={"nomeArquivo": filename},
        timeout=120,
    )
    download_resp.raise_for_status()
    if "application/json" in download_resp.headers.get("content-type", ""):
        try:
            error_payload = download_resp.json()
        except ValueError:
            error_payload = {}
        raise RuntimeError(error_payload.get("message") or "TMS não liberou o download do relatório.")

    return download_resp.content, filename


def _do_tms_sync(days: int) -> dict:
    try:
        import requests as _req
    except ImportError:
        raise RuntimeError("Biblioteca 'requests' não instalada. Execute: pip install requests")

    login, senha = _load_tms_credentials()
    session = _req.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FranquiaBipagem/2.0)"})

    _tms_login(session, login, senha)
    csv_bytes, filename = _download_tms_report(session, days)
    stats = import_csv_bytes(csv_bytes, filename)
    return stats


def _run_sync_job(days: int) -> None:
    global _sync_state
    with _sync_lock:
        if _sync_state["status"] == "running":
            return
        _sync_state = {"status": "running", "message": "Sincronizando com TMS…", "updated": now_text()}
    try:
        stats = _do_tms_sync(days)
        msg = f"Concluído: {stats['imported_rows']} linhas processadas, {stats['inserted_rows']} novos, {stats['updated_rows']} atualizados."
        _sync_state = {"status": "done", "message": msg, "updated": now_text()}
    except Exception as exc:
        _sync_state = {"status": "error", "message": f"Erro na sincronização: {exc}", "updated": now_text()}


def _start_auto_sync() -> None:
    def _loop() -> None:
        # wait 30s after startup before first run so DB is fully ready
        time.sleep(30)
        while True:
            try:
                _run_sync_job(2)
            except Exception:
                pass
            time.sleep(6 * 3600)
    t = threading.Thread(target=_loop, daemon=True, name="tms-auto-sync")
    t.start()


def clean_cell(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\ufeff", "").strip()
    if len(text) >= 3 and text.startswith('="') and text.endswith('"'):
        text = text[2:-1]
    elif len(text) >= 2 and text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    return text.strip()


def pedido_key(value: object) -> str:
    return "".join(clean_cell(value).upper().split())


def decode_csv(data: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=60000")
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS pedidos (
            pedido_key TEXT PRIMARY KEY,
            pedido TEXT NOT NULL,
            franquia TEXT,
            sigla_unidade_entrega TEXT,
            sigla_unidade_atual TEXT,
            sigla_unidade_coleta TEXT,
            nro_entrega TEXT,
            nro_arquivo TEXT,
            cliente TEXT,
            status TEXT,
            cep TEXT,
            cidade TEXT,
            uf TEXT,
            dt_cadastro TEXT,
            imported_at TEXT NOT NULL,
            source_file TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_pedidos_pedido ON pedidos (pedido);
        CREATE INDEX IF NOT EXISTS idx_pedidos_franquia ON pedidos (franquia);

        CREATE TABLE IF NOT EXISTS imports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file TEXT NOT NULL,
            imported_at TEXT NOT NULL,
            total_rows INTEGER NOT NULL,
            imported_rows INTEGER NOT NULL,
            inserted_rows INTEGER NOT NULL,
            updated_rows INTEGER NOT NULL,
            skipped_rows INTEGER NOT NULL,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS int_export_queue (
            pedido_key TEXT PRIMARY KEY,
            pedido TEXT NOT NULL,
            nro_entrega TEXT,
            sigla_unidade_entrega TEXT NOT NULL,
            scanned_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_int_export_scanned_at
        ON int_export_queue (scanned_at DESC);

        CREATE TABLE IF NOT EXISTS int_export_queue_items (
            queue_id TEXT NOT NULL,
            pedido_key TEXT NOT NULL,
            pedido TEXT NOT NULL,
            nro_entrega TEXT,
            sigla_unidade_entrega TEXT NOT NULL,
            scanned_at TEXT NOT NULL,
            PRIMARY KEY (queue_id, pedido_key)
        );

        CREATE INDEX IF NOT EXISTS idx_int_export_items_queue_scanned_at
        ON int_export_queue_items (queue_id, scanned_at DESC);

        CREATE TABLE IF NOT EXISTS other_export_queue_items (
            pedido_key TEXT PRIMARY KEY,
            pedido TEXT NOT NULL,
            nro_entrega TEXT,
            sigla_unidade_entrega TEXT NOT NULL,
            scanned_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_other_export_scanned_at
        ON other_export_queue_items (scanned_at DESC);
        """
    )
    conn.commit()


def ensure_queue_id(raw_value: object) -> str:
    value = clean_cell(raw_value)
    if not value:
        return f"queue-{uuid4().hex}"
    normalized = re.sub(r"[^A-Za-z0-9_-]", "", value)
    if not normalized:
        return f"queue-{uuid4().hex}"
    return normalized[:80]


def row_value(row: dict[str, str], column: str) -> str:
    return clean_cell(row.get(column, ""))


def import_csv_bytes(data: bytes, filename: str) -> dict[str, object]:
    csv.field_size_limit(sys.maxsize)
    text = decode_csv(data)
    reader = csv.DictReader(io.StringIO(text), delimiter=";")

    if not reader.fieldnames:
        raise ValueError("Arquivo CSV sem cabecalho.")

    headers = [clean_cell(name) for name in reader.fieldnames]
    missing = [name for name in (ORDER_COLUMN, FRANCHISE_COLUMN) if name not in headers]
    if missing:
        raise ValueError("Coluna obrigatoria ausente: " + ", ".join(missing))

    total = imported = inserted = updated = skipped = 0
    imported_at = now_text()

    with connect() as conn:
        cur = conn.cursor()
        for raw_row in reader:
            total += 1
            row = {clean_cell(k): v for k, v in raw_row.items() if k is not None}
            pedido = row_value(row, ORDER_COLUMN)
            key = pedido_key(pedido)
            if not key:
                skipped += 1
                continue

            sigla_entrega = row_value(row, FRANCHISE_COLUMN)
            sigla_atual = row_value(row, FALLBACK_FRANCHISE_COLUMN)
            franquia = sigla_entrega or sigla_atual

            already_exists = cur.execute(
                "SELECT 1 FROM pedidos WHERE pedido_key = ?",
                (key,),
            ).fetchone()

            cur.execute(
                """
                INSERT INTO pedidos (
                    pedido_key, pedido, franquia, sigla_unidade_entrega,
                    sigla_unidade_atual, sigla_unidade_coleta, nro_entrega,
                    nro_arquivo, cliente, status, cep, cidade, uf, dt_cadastro,
                    imported_at, source_file
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pedido_key) DO UPDATE SET
                    pedido = excluded.pedido,
                    franquia = excluded.franquia,
                    sigla_unidade_entrega = excluded.sigla_unidade_entrega,
                    sigla_unidade_atual = excluded.sigla_unidade_atual,
                    sigla_unidade_coleta = excluded.sigla_unidade_coleta,
                    nro_entrega = excluded.nro_entrega,
                    nro_arquivo = excluded.nro_arquivo,
                    cliente = excluded.cliente,
                    status = excluded.status,
                    cep = excluded.cep,
                    cidade = excluded.cidade,
                    uf = excluded.uf,
                    dt_cadastro = excluded.dt_cadastro,
                    imported_at = excluded.imported_at,
                    source_file = excluded.source_file
                """,
                (
                    key,
                    pedido,
                    franquia,
                    sigla_entrega,
                    sigla_atual,
                    row_value(row, "Sigla Unidade Coleta de Carga"),
                    row_value(row, "Nro. Entrega"),
                    row_value(row, "Nro. Arquivo"),
                    row_value(row, "Cliente"),
                    row_value(row, "Status"),
                    row_value(row, "CEP Pessoa Visita"),
                    row_value(row, "Cidade Pessoa Visita"),
                    row_value(row, "UF Pessoa Visita"),
                    row_value(row, "Dt. Cadastro"),
                    imported_at,
                    filename,
                ),
            )
            imported += 1
            if already_exists:
                updated += 1
            else:
                inserted += 1

        conn.execute(
            """
            INSERT INTO imports (
                source_file, imported_at, total_rows, imported_rows,
                inserted_rows, updated_rows, skipped_rows, error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (filename, imported_at, total, imported, inserted, updated, skipped),
        )
        conn.commit()

    return {
        "source_file": filename,
        "imported_at": imported_at,
        "total_rows": total,
        "imported_rows": imported,
        "inserted_rows": inserted,
        "updated_rows": updated,
        "skipped_rows": skipped,
    }


def lookup_pedido(numero: str) -> dict[str, object]:
    key = pedido_key(numero)
    if not key:
        return {"found": False, "message": "Informe um numero de pedido."}

    with connect() as conn:
        row = conn.execute(
            """
            SELECT pedido, franquia, sigla_unidade_entrega, sigla_unidade_atual,
                   sigla_unidade_coleta, nro_entrega, nro_arquivo, cliente,
                   status, cep, cidade, uf, dt_cadastro, imported_at, source_file
            FROM pedidos
            WHERE pedido_key = ?
            """,
            (key,),
        ).fetchone()

    if not row:
        return {
            "found": False,
            "pedido": clean_cell(numero),
            "message": "Pedido nao encontrado no banco importado.",
        }

    result = dict(row)
    result["found"] = True
    return result


def get_stats(queue_id: str | None = None) -> dict[str, object]:
    resolved_queue_id = ensure_queue_id(queue_id)
    with connect() as conn:
        totals = conn.execute(
            """
            SELECT
                COUNT(*) AS pedidos,
                COUNT(DISTINCT franquia) AS franquias,
                MAX(imported_at) AS ultima_atualizacao
            FROM pedidos
            """
        ).fetchone()
        imports = conn.execute(
            """
            SELECT source_file, imported_at, total_rows, imported_rows,
                   inserted_rows, updated_rows, skipped_rows
            FROM imports
            ORDER BY id DESC
            LIMIT 8
            """
        ).fetchall()
        top = conn.execute(
            """
            SELECT COALESCE(NULLIF(franquia, ''), '(sem sigla)') AS franquia,
                   COUNT(*) AS total
            FROM pedidos
            GROUP BY COALESCE(NULLIF(franquia, ''), '(sem sigla)')
            ORDER BY total DESC
            LIMIT 10
            """
        ).fetchall()
        int_queue = conn.execute(
            "SELECT COUNT(*) AS total FROM int_export_queue_items WHERE queue_id = ?",
            (resolved_queue_id,),
        ).fetchone()
        other_queue = conn.execute(
            "SELECT COUNT(*) AS total FROM other_export_queue_items",
        ).fetchone()

    return {
        "pedidos": totals["pedidos"] or 0,
        "franquias": totals["franquias"] or 0,
        "ultima_atualizacao": totals["ultima_atualizacao"],
        "int_queue_count": int_queue["total"] or 0,
        "other_queue_count": other_queue["total"] or 0,
        "queue_id": resolved_queue_id,
        "imports": [dict(row) for row in imports],
        "top_franquias": [dict(row) for row in top],
    }


def register_int_scan(numero: str, queue_id: str | None = None) -> dict[str, object]:
    key = pedido_key(numero)
    if not key:
        return {"queued": False, "message": "Numero de pedido vazio."}
    resolved_queue_id = ensure_queue_id(queue_id)

    with connect() as conn:
        row = conn.execute(
            """
            SELECT pedido_key, pedido, nro_entrega, sigla_unidade_entrega
            FROM pedidos
            WHERE pedido_key = ?
            """,
            (key,),
        ).fetchone()

        if not row:
            return {"queued": False, "message": "Pedido nao encontrado."}

        if clean_cell(row["sigla_unidade_entrega"]).upper() != "INT":
            count = conn.execute(
                "SELECT COUNT(*) AS total FROM int_export_queue_items WHERE queue_id = ?",
                (resolved_queue_id,),
            ).fetchone()["total"] or 0
            return {
                "queued": False,
                "message": "Pedido fora do lote INT.",
                "pending_count": count,
                "queue_id": resolved_queue_id,
            }

        already_exists = conn.execute(
            "SELECT 1 FROM int_export_queue_items WHERE queue_id = ? AND pedido_key = ?",
            (resolved_queue_id, key),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO int_export_queue_items (
                queue_id, pedido_key, pedido, nro_entrega, sigla_unidade_entrega, scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(queue_id, pedido_key) DO UPDATE SET
                pedido = excluded.pedido,
                nro_entrega = excluded.nro_entrega,
                sigla_unidade_entrega = excluded.sigla_unidade_entrega,
                scanned_at = excluded.scanned_at
            """,
            (
                resolved_queue_id,
                row["pedido_key"],
                clean_cell(row["pedido"]),
                clean_cell(row["nro_entrega"]),
                clean_cell(row["sigla_unidade_entrega"]),
                now_text(),
            ),
        )
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) AS total FROM int_export_queue_items WHERE queue_id = ?",
            (resolved_queue_id,),
        ).fetchone()["total"] or 0

    return {
        "queued": True,
        "added": not bool(already_exists),
        "pedido": clean_cell(row["pedido"]),
        "nro_entrega": clean_cell(row["nro_entrega"]),
        "sigla_unidade_entrega": clean_cell(row["sigla_unidade_entrega"]),
        "pending_count": count,
        "queue_id": resolved_queue_id,
    }


def register_other_scan(numero: str) -> dict[str, object]:
    key = pedido_key(numero)
    if not key:
        return {"queued": False, "message": "Numero de pedido vazio."}

    with connect() as conn:
        row = conn.execute(
            """
            SELECT pedido_key, pedido, nro_entrega, sigla_unidade_entrega
            FROM pedidos
            WHERE pedido_key = ?
            """,
            (key,),
        ).fetchone()

        if not row:
            return {"queued": False, "message": "Pedido nao encontrado."}

        sigla = clean_cell(row["sigla_unidade_entrega"]).upper()
        if not sigla:
            count = conn.execute("SELECT COUNT(*) AS total FROM other_export_queue_items").fetchone()["total"] or 0
            return {"queued": False, "message": "Pedido sem unidade de entrega.", "pending_count": count}

        if sigla == "INT":
            count = conn.execute("SELECT COUNT(*) AS total FROM other_export_queue_items").fetchone()["total"] or 0
            return {"queued": False, "message": "Pedido pertence ao lote INT.", "pending_count": count}

        already_exists = conn.execute(
            "SELECT 1 FROM other_export_queue_items WHERE pedido_key = ?",
            (key,),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO other_export_queue_items (
                pedido_key, pedido, nro_entrega, sigla_unidade_entrega, scanned_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(pedido_key) DO UPDATE SET
                pedido = excluded.pedido,
                nro_entrega = excluded.nro_entrega,
                sigla_unidade_entrega = excluded.sigla_unidade_entrega,
                scanned_at = excluded.scanned_at
            """,
            (
                row["pedido_key"],
                clean_cell(row["pedido"]),
                clean_cell(row["nro_entrega"]),
                clean_cell(row["sigla_unidade_entrega"]),
                now_text(),
            ),
        )
        conn.commit()
        count = conn.execute("SELECT COUNT(*) AS total FROM other_export_queue_items").fetchone()["total"] or 0

    return {
        "queued": True,
        "added": not bool(already_exists),
        "pedido": clean_cell(row["pedido"]),
        "nro_entrega": clean_cell(row["nro_entrega"]),
        "sigla_unidade_entrega": clean_cell(row["sigla_unidade_entrega"]),
        "pending_count": count,
    }


def normalized_sigla(value: object) -> str:
    return clean_cell(value).upper()


def get_other_queue_data(sigla: str | None = None) -> dict[str, object]:
    resolved_sigla = normalized_sigla(sigla)
    with connect() as conn:
        summary_rows = conn.execute(
            """
            SELECT sigla_unidade_entrega AS sigla, COUNT(*) AS total
            FROM other_export_queue_items
            GROUP BY sigla_unidade_entrega
            ORDER BY sigla_unidade_entrega
            """
        ).fetchall()

        if resolved_sigla:
            rows = conn.execute(
                """
                SELECT
                    q.pedido,
                    q.nro_entrega,
                    q.sigla_unidade_entrega,
                    q.scanned_at,
                    COALESCE(NULLIF(TRIM(p.cliente), ''), 'Sem Cliente') AS cliente
                FROM other_export_queue_items q
                LEFT JOIN pedidos p ON p.pedido_key = q.pedido_key
                WHERE UPPER(TRIM(q.sigla_unidade_entrega)) = ?
                ORDER BY q.scanned_at DESC, q.pedido DESC
                LIMIT 300
                """,
                (resolved_sigla,),
            ).fetchall()
            filtered_total_row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM other_export_queue_items
                WHERE UPPER(TRIM(sigla_unidade_entrega)) = ?
                """,
                (resolved_sigla,),
            ).fetchone()
            filtered_total = filtered_total_row["total"] or 0
        else:
            rows = conn.execute(
                """
                SELECT
                    q.pedido,
                    q.nro_entrega,
                    q.sigla_unidade_entrega,
                    q.scanned_at,
                    COALESCE(NULLIF(TRIM(p.cliente), ''), 'Sem Cliente') AS cliente
                FROM other_export_queue_items q
                LEFT JOIN pedidos p ON p.pedido_key = q.pedido_key
                ORDER BY q.scanned_at DESC, q.pedido DESC
                LIMIT 300
                """
            ).fetchall()
            filtered_total = sum((row["total"] or 0) for row in summary_rows)

    summary = [dict(row) for row in summary_rows]
    total = sum((row["total"] or 0) for row in summary_rows)
    return {
        "total": total,
        "filtered_total": filtered_total,
        "selected_sigla": resolved_sigla,
        "franquias": summary,
        "rows": [dict(row) for row in rows],
    }


def export_int_workbook(queue_id: str | None = None) -> tuple[str, bytes]:
    resolved_queue_id = ensure_queue_id(queue_id)
    with connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS total FROM int_export_queue_items WHERE queue_id = ?",
            (resolved_queue_id,),
        ).fetchone()
        if not row or not row["total"]:
            raise ValueError("Nao ha registros INT aguardando exportacao.")

    output_path = DATA_DIR / f"lote-int-{datetime.now().strftime('%Y%m%d-%H%M%S')}.xlsx"
    subprocess.run(
        [
            str(BUNDLED_PYTHON),
            str(TOOLS_DIR / "export_int_workbook.py"),
            "--db",
            str(DB_PATH),
            "--out",
            str(output_path),
            "--queue-id",
            resolved_queue_id,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    body = output_path.read_bytes()

    with connect() as conn:
        conn.execute("DELETE FROM int_export_queue_items WHERE queue_id = ?", (resolved_queue_id,))
        conn.commit()

    try:
        output_path.unlink(missing_ok=True)
    except PermissionError:
        pass

    return output_path.name, body


def export_other_workbook(sigla: str | None = None) -> tuple[str, bytes]:
    resolved_sigla = normalized_sigla(sigla)
    with connect() as conn:
        if resolved_sigla:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM other_export_queue_items
                WHERE UPPER(TRIM(sigla_unidade_entrega)) = ?
                """,
                (resolved_sigla,),
            ).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) AS total FROM other_export_queue_items").fetchone()
        if not row or not row["total"]:
            raise ValueError("Nao ha registros das demais franquias aguardando exportacao.")

    file_stub = resolved_sigla.lower() if resolved_sigla else "todas"
    output_path = DATA_DIR / f"lote-demais-franquias-{file_stub}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.xlsx"
    subprocess.run(
        [
            str(BUNDLED_PYTHON),
            str(TOOLS_DIR / "export_other_workbook.py"),
            "--db",
            str(DB_PATH),
            "--out",
            str(output_path),
            *(["--sigla", resolved_sigla] if resolved_sigla else []),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    body = output_path.read_bytes()

    with connect() as conn:
        if resolved_sigla:
            conn.execute(
                "DELETE FROM other_export_queue_items WHERE UPPER(TRIM(sigla_unidade_entrega)) = ?",
                (resolved_sigla,),
            )
        else:
            conn.execute("DELETE FROM other_export_queue_items")
        conn.commit()

    try:
        output_path.unlink(missing_ok=True)
    except PermissionError:
        pass

    return output_path.name, body


def parse_uploaded_file(handler: BaseHTTPRequestHandler) -> tuple[str, bytes]:
    content_type = handler.headers.get("Content-Type", "")
    length = int(handler.headers.get("Content-Length", "0") or "0")
    match = re.search(r'boundary=(?:"([^"]+)"|([^;]+))', content_type)
    if not match:
        raise ValueError("Requisicao de upload sem boundary multipart.")

    boundary = (match.group(1) or match.group(2)).encode("latin1")
    body = handler.rfile.read(length)
    delimiter = b"--" + boundary

    for part in body.split(delimiter):
        part = part.strip(b"\r\n")
        if not part or part == b"--":
            continue
        if part.endswith(b"--"):
            part = part[:-2].rstrip(b"\r\n")

        header_blob, separator, content = part.partition(b"\r\n\r\n")
        if not separator:
            header_blob, separator, content = part.partition(b"\n\n")
        if not separator:
            continue

        header_text = header_blob.decode("latin1", errors="replace")
        disposition = next(
            (line for line in header_text.splitlines() if line.lower().startswith("content-disposition:")),
            "",
        )
        if 'name="file"' not in disposition:
            continue

        filename_match = re.search(r'filename="([^"]*)"', disposition)
        filename = Path(filename_match.group(1) if filename_match else "relatorio.csv").name
        return filename, content

    raise ValueError("Nenhum arquivo enviado.")


def json_response(handler: BaseHTTPRequestHandler, payload: object, status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def html_response(handler: BaseHTTPRequestHandler, body: str, status: int = 200) -> None:
    data = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def bytes_response(
    handler: BaseHTTPRequestHandler,
    filename: str,
    body: bytes,
    content_type: str,
) -> None:
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Disposition", f'attachment; filename="{filename}"')
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def parse_json_body(handler: BaseHTTPRequestHandler) -> dict[str, object]:
    content_length = int(handler.headers.get("Content-Length", "0") or "0")
    raw = handler.rfile.read(content_length) if content_length > 0 else b"{}"
    return json.loads(raw.decode("utf-8") or "{}")


def page() -> str:
    return r"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bipagem de Franquia</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b1220;
      --bg-2: #111a2c;
      --surface: #121c2f;
      --surface-2: #182338;
      --surface-3: #0f1727;
      --text: #edf3ff;
      --muted: #96a4bc;
      --line: #263349;
      --line-strong: #31415d;
      --accent: #17b4a1;
      --accent-2: #129180;
      --accent-soft: rgba(23, 180, 161, 0.12);
      --danger: #ff7b72;
      --warn: #f5b942;
      --ok-bg: #0d2b2a;
      --bad-bg: #35181d;
      --ok-line: #1d6e67;
      --bad-line: #7f3843;
      --shadow: 0 18px 40px rgba(0, 0, 0, 0.28);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI Variable Text", "Segoe UI", Arial, Helvetica, sans-serif;
      line-height: 1.45;
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
      background:
        radial-gradient(circle at top, rgba(23, 180, 161, 0.08), transparent 32%),
        linear-gradient(180deg, var(--bg-2) 0%, var(--bg) 100%);
      color: var(--text);
    }
    header {
      background: rgba(8, 14, 24, 0.86);
      backdrop-filter: blur(10px);
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
      color: #fff;
      padding: 18px 28px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      position: sticky;
      top: 0;
      z-index: 5;
    }
    header a {
      color: #d8fff8;
      font-weight: 600;
      text-decoration: none;
      background: rgba(23, 180, 161, 0.12);
      border: 1px solid rgba(23, 180, 161, 0.24);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
    }
    h1 { font-size: 24px; margin: 0; font-weight: 700; letter-spacing: 0; }
    main {
      display: grid;
      grid-template-columns: minmax(380px, 1fr) 420px;
      gap: 18px;
      padding: 22px 18px 28px;
      max-width: 1440px;
      margin: 0 auto;
    }
    section {
      background: var(--surface);
      border: 1px solid rgba(255, 255, 255, 0.06);
      box-shadow: var(--shadow);
      border-radius: 10px;
      padding: 20px 20px 18px;
    }
    h2 {
      font-size: 17px;
      margin: 0 0 16px;
      letter-spacing: 0;
      color: #f5f8ff;
      font-weight: 700;
    }
    label {
      display: block;
      font-size: 12px;
      font-weight: 700;
      color: var(--muted);
      margin-bottom: 8px;
      text-transform: uppercase;
    }
    input[type="text"], input[type="file"] {
      width: 100%;
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      padding: 13px 14px;
      font-size: 17px;
      background: var(--surface-3);
      color: var(--text);
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.02);
    }
    input::placeholder { color: #6f7f99; }
    input[type="text"]:focus {
      outline: 3px solid rgba(23, 180, 161, 0.16);
      border-color: var(--accent);
    }
    button {
      appearance: none;
      border: 0;
      border-radius: 8px;
      background: var(--accent);
      color: #fff;
      padding: 12px 14px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      box-shadow: 0 10px 22px rgba(23, 180, 161, 0.18);
      transition: transform 120ms ease, background 120ms ease, box-shadow 120ms ease;
    }
    button:hover { background: var(--accent-2); transform: translateY(-1px); }
    button:disabled { opacity: 0.58; cursor: wait; }
    .scan-row {
      display: grid;
      grid-template-columns: 1fr 150px;
      gap: 10px;
      align-items: end;
    }
    .upload-actions {
      display: grid;
      grid-template-columns: 1fr 120px;
      gap: 10px;
      align-items: end;
    }
    .result {
      margin-top: 18px;
      border: 1px solid rgba(255, 255, 255, 0.06);
      border-radius: 10px;
      min-height: 280px;
      background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01)), var(--surface-2);
      display: grid;
      align-content: center;
      justify-items: center;
      text-align: center;
      padding: 28px;
    }
    .result.ok { background: linear-gradient(180deg, rgba(23,180,161,0.09), rgba(23,180,161,0.02)), var(--ok-bg); border-color: var(--ok-line); }
    .result.bad { background: linear-gradient(180deg, rgba(255,123,114,0.10), rgba(255,123,114,0.02)), var(--bad-bg); border-color: var(--bad-line); }
    .franquia {
      font-size: clamp(56px, 12vw, 150px);
      line-height: 0.92;
      font-weight: 800;
      color: #d7fff8;
      text-shadow: 0 10px 24px rgba(0, 0, 0, 0.28);
      word-break: break-word;
      max-width: 100%;
    }
    .not-found {
      color: var(--danger);
      font-size: clamp(32px, 7vw, 72px);
      font-weight: 800;
      line-height: 1;
    }
    .pedido-line {
      margin-top: 12px;
      font-size: 22px;
      font-weight: 700;
      color: #d7deea;
      overflow-wrap: anywhere;
    }
    .details {
      display: grid;
      grid-template-columns: repeat(4, minmax(120px, 1fr));
      gap: 12px;
      margin-top: 16px;
    }
    .detail {
      background: var(--surface-3);
      border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 8px;
      padding: 12px;
      min-height: 72px;
    }
    .detail span {
      display: block;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      margin-bottom: 6px;
      text-transform: uppercase;
    }
    .detail strong {
      display: block;
      font-size: 15px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }
    .side {
      display: grid;
      gap: 18px;
      align-content: start;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 10px;
    }
    .stat {
      background: var(--surface-2);
      border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 8px;
      padding: 14px;
    }
    .stat b {
      display: block;
      font-size: 26px;
      line-height: 1.1;
      color: #f5f8ff;
    }
    .stat span {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }
    .message {
      margin-top: 14px;
      min-height: 22px;
      font-size: 13px;
      color: var(--muted);
    }
    .message.error { color: var(--danger); font-weight: 700; }
    .message.warn { color: var(--warn); font-weight: 700; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }
    th, td {
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }
    th {
      color: var(--muted);
      font-size: 11px;
      background: rgba(255, 255, 255, 0.03);
      text-transform: uppercase;
    }
    tbody tr:hover { background: rgba(255, 255, 255, 0.025); }
    .history { margin-top: 18px; }
    .history tbody td:first-child {
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    .pill {
      display: inline-block;
      padding: 4px 7px;
      border-radius: 999px;
      background: rgba(23, 180, 161, 0.14);
      color: #c7fff7;
      border: 1px solid rgba(23, 180, 161, 0.18);
      font-weight: 700;
    }
    @media (max-width: 980px) {
      main { grid-template-columns: 1fr; }
      .details { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
    }
    @media (max-width: 560px) {
      header { align-items: flex-start; flex-direction: column; }
      main { padding: 10px; gap: 10px; }
      section { padding: 14px; }
      .scan-row, .upload-actions { grid-template-columns: 1fr; }
      .details, .stats { grid-template-columns: 1fr; }
      .result { min-height: 220px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Bipagem de Franquia</h1>
    <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
      <a href="/demais-franquias">Demais franquias</a>
      <div id="clock"></div>
    </div>
  </header>
  <main>
    <section>
      <h2>Consulta operacional</h2>
      <form id="scanForm" autocomplete="off">
        <div class="scan-row">
          <div>
            <label for="scanInput">Número do pedido</label>
            <input id="scanInput" type="text" inputmode="text" autofocus placeholder="Bipe ou digite o pedido">
          </div>
          <button id="scanButton" type="submit">Consultar</button>
        </div>
      </form>

      <div id="result" class="result">
        <div>
          <div class="franquia">---</div>
          <div class="pedido-line">Aguardando bipagem</div>
        </div>
      </div>

      <div id="details" class="details" aria-live="polite"></div>

      <div class="history">
        <h2>Últimas bipagens</h2>
        <table>
          <thead>
            <tr>
              <th>Pedido</th>
              <th>Franquia</th>
              <th>Status</th>
              <th>Hora</th>
            </tr>
          </thead>
          <tbody id="historyBody"></tbody>
        </table>
      </div>
    </section>

    <div class="side">
      <section>
        <h2>Sincronizar com TMS</h2>
        <button id="syncButton" type="button">Sincronizar com TMS</button>
        <div id="syncMessage" class="message">Puxa as últimas 2 semanas do TMS Elite e atualiza a base.</div>
      </section>

      <section>
        <h2>Importar relatório</h2>
        <form id="uploadForm">
          <div class="upload-actions">
            <div>
              <label for="csvFiles">CSV do TMS</label>
              <input id="csvFiles" name="file" type="file" accept=".csv,text/csv" multiple>
            </div>
            <button id="uploadButton" type="submit">Importar</button>
          </div>
          <div id="uploadMessage" class="message">Suba aqui o relatório para atualizar a base de todas as máquinas.</div>
        </form>
      </section>

      <section>
        <h2>Banco acumulado</h2>
        <div class="stats">
          <div class="stat"><b id="statPedidos">0</b><span>Pedidos</span></div>
          <div class="stat"><b id="statFranquias">0</b><span>Franquias</span></div>
          <div class="stat"><b id="statAtualizado">-</b><span>Última carga</span></div>
        </div>
      </section>

      <section>
        <h2>Lote INT</h2>
        <div class="stats">
          <div class="stat"><b id="intQueueCount">0</b><span>INT bipados</span></div>
        </div>
        <div class="message" id="intQueueMessage">Somente pedidos com unidade de entrega INT entram no lote desta máquina.</div>
        <button id="exportIntButton" type="button">Exportar Excel INT</button>
      </section>

      <section>
        <h2>Demais franquias</h2>
        <div class="stats">
          <div class="stat"><b id="otherQueueCount">0</b><span>Demais bipados</span></div>
        </div>
        <div class="message" id="otherQueueMessage">Todas as máquinas alimentam juntas este lote. A separação e exportação ficam na página própria.</div>
        <button id="openOtherPageButton" type="button">Abrir página das demais</button>
      </section>

      <section>
        <h2>Últimas importações</h2>
        <table>
          <thead>
            <tr>
              <th>Arquivo</th>
              <th>Novos</th>
              <th>Atualizados</th>
            </tr>
          </thead>
          <tbody id="importsBody"></tbody>
        </table>
      </section>

      <section>
        <h2>Franquias mais frequentes</h2>
        <table>
          <thead>
            <tr>
              <th>Sigla</th>
              <th>Total</th>
            </tr>
          </thead>
          <tbody id="topBody"></tbody>
        </table>
      </section>
    </div>
  </main>

  <script>
    const scanForm = document.querySelector("#scanForm");
    const scanInput = document.querySelector("#scanInput");
    const scanButton = document.querySelector("#scanButton");
    const result = document.querySelector("#result");
    const details = document.querySelector("#details");
    const historyBody = document.querySelector("#historyBody");
    const uploadForm = document.querySelector("#uploadForm");
    const uploadButton = document.querySelector("#uploadButton");
    const uploadMessage = document.querySelector("#uploadMessage");
    const intQueueCount = document.querySelector("#intQueueCount");
    const intQueueMessage = document.querySelector("#intQueueMessage");
    const exportIntButton = document.querySelector("#exportIntButton");
    const otherQueueCount = document.querySelector("#otherQueueCount");
    const otherQueueMessage = document.querySelector("#otherQueueMessage");
    const openOtherPageButton = document.querySelector("#openOtherPageButton");
    const history = [];
    const queueStorageKey = "franquiaBipagemIntQueueId";
    let queueId = localStorage.getItem(queueStorageKey) || "";

    function generateQueueId() {
      const parts = [
        Date.now().toString(36),
        Math.random().toString(36).slice(2, 10),
        Math.random().toString(36).slice(2, 10)
      ].filter(Boolean);
      return `queue-${parts.join("")}`;
    }

    function ensureQueueId() {
      if (!queueId) {
        queueId = generateQueueId();
        localStorage.setItem(queueStorageKey, queueId);
      }
      return queueId;
    }

    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, char => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
      }[char]));
    }

    function shortDate(value) {
      if (!value) return "-";
      return String(value).replace("T", " ").slice(0, 16);
    }

    function renderDetails(data) {
      const items = [
        ["Cidade/UF", [data.cidade, data.uf].filter(Boolean).join(" / ")],
        ["CEP", data.cep],
        ["Status", data.status],
        ["Cadastro", data.dt_cadastro],
        ["Unid. entrega", data.sigla_unidade_entrega],
        ["Unid. atual", data.sigla_unidade_atual],
        ["Cliente", data.cliente],
        ["Fonte", data.source_file]
      ];
      details.innerHTML = items.map(([label, value]) => `
        <div class="detail">
          <span>${esc(label)}</span>
          <strong>${esc(value || "-")}</strong>
        </div>
      `).join("");
    }

    function renderHistory() {
      historyBody.innerHTML = history.slice(0, 10).map(item => `
        <tr>
          <td>${esc(item.pedido)}</td>
          <td><span class="pill">${esc(item.franquia || "-")}</span></td>
          <td>${esc(item.status || "-")}</td>
          <td>${esc(item.hora)}</td>
        </tr>
      `).join("");
    }

    function renderIntQueue(count, message) {
      intQueueCount.textContent = count ?? 0;
      exportIntButton.disabled = !count;
      if (message) {
        intQueueMessage.className = "message";
        intQueueMessage.textContent = message;
      }
    }

    function renderOtherQueue(count, message) {
      otherQueueCount.textContent = count ?? 0;
      if (message) {
        otherQueueMessage.className = "message";
        otherQueueMessage.textContent = message;
      }
    }

    async function handleIntQueue(data) {
      if ((data.sigla_unidade_entrega || "").toUpperCase() !== "INT") {
        renderIntQueue(Number(intQueueCount.textContent || "0"), "Pedido fora do lote INT.");
        return;
      }
      try {
        const response = await fetch("/api/scan-int", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ numero: data.pedido, queue_id: ensureQueueId() })
        });
        const queueData = await response.json();
        if (queueData.queue_id) {
          queueId = queueData.queue_id;
          localStorage.setItem(queueStorageKey, queueId);
        }
        renderIntQueue(
          queueData.pending_count ?? 0,
          queueData.added === false
            ? "Pedido INT ja estava no lote desta maquina."
            : "Pedido INT adicionado ao lote desta maquina."
        );
      } catch (error) {
        renderIntQueue(Number(intQueueCount.textContent || "0"), "Falha ao registrar pedido INT.");
      }
    }

    async function handleOtherQueue(data) {
      if ((data.sigla_unidade_entrega || "").toUpperCase() === "INT") {
        renderOtherQueue(Number(otherQueueCount.textContent || "0"), "Pedido INT fica fora do lote das demais franquias.");
        return;
      }
      try {
        const response = await fetch("/api/scan-other", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ numero: data.pedido })
        });
        const queueData = await response.json();
        renderOtherQueue(
          queueData.pending_count ?? 0,
          queueData.added === false
            ? "Pedido já estava no lote geral das demais franquias."
            : "Pedido adicionado ao lote geral das demais franquias."
        );
      } catch (error) {
        renderOtherQueue(Number(otherQueueCount.textContent || "0"), "Falha ao registrar pedido nas demais franquias.");
      }
    }

    function setResultFound(data) {
      result.className = "result ok";
      result.innerHTML = `
        <div>
          <div class="franquia">${esc(data.franquia || "SEM SIGLA")}</div>
          <div class="pedido-line">Pedido ${esc(data.pedido)}</div>
        </div>
      `;
      renderDetails(data);
      handleIntQueue(data);
      handleOtherQueue(data);
      history.unshift({
        pedido: data.pedido,
        franquia: data.franquia,
        status: data.status,
        hora: new Date().toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit", second: "2-digit" })
      });
      renderHistory();
    }

    function setResultMissing(numero, message) {
      result.className = "result bad";
      result.innerHTML = `
        <div>
          <div class="not-found">NÃO ENCONTRADO</div>
          <div class="pedido-line">${esc(numero)}</div>
          <div class="message error">${esc(message)}</div>
        </div>
      `;
      details.innerHTML = "";
      history.unshift({
        pedido: numero,
        franquia: "NÃO ENCONTRADO",
        status: message,
        hora: new Date().toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit", second: "2-digit" })
      });
      renderHistory();
    }

    async function lookup(numero) {
      const value = numero.trim();
      if (!value) return;
      scanButton.disabled = true;
      try {
        const response = await fetch(`/api/pedido?numero=${encodeURIComponent(value)}`);
        const data = await response.json();
        if (data.found) setResultFound(data);
        else setResultMissing(value, data.message || "Pedido nao encontrado.");
      } catch (error) {
        setResultMissing(value, "Falha ao consultar o banco.");
      } finally {
        scanButton.disabled = false;
        scanInput.value = "";
        scanInput.focus();
      }
    }

    scanForm.addEventListener("submit", event => {
      event.preventDefault();
      lookup(scanInput.value);
    });

    uploadForm.addEventListener("submit", async event => {
      event.preventDefault();
      const files = Array.from(document.querySelector("#csvFiles").files || []);
      if (!files.length) {
        uploadMessage.className = "message warn";
        uploadMessage.textContent = "Selecione pelo menos um CSV.";
        return;
      }

      uploadButton.disabled = true;
      uploadMessage.className = "message";
      uploadMessage.textContent = "Importando...";

      const results = [];
      try {
        for (const file of files) {
          const formData = new FormData();
          formData.append("file", file);
          const response = await fetch("/api/import", { method: "POST", body: formData });
          const data = await response.json();
          if (!response.ok) throw new Error(data.error || `Falha ao importar ${file.name}`);
          results.push(`${file.name}: ${data.inserted_rows} novos, ${data.updated_rows} atualizados`);
        }
        uploadMessage.className = "message";
        uploadMessage.textContent = results.join(" | ");
        uploadForm.reset();
        await loadStats();
        scanInput.focus();
      } catch (error) {
        uploadMessage.className = "message error";
        uploadMessage.textContent = error.message;
      } finally {
        uploadButton.disabled = false;
      }
    });

    exportIntButton.addEventListener("click", async () => {
      exportIntButton.disabled = true;
      intQueueMessage.className = "message";
      intQueueMessage.textContent = "Gerando Excel INT...";
      try {
        const response = await fetch("/api/export-int-workbook", {
          method: "POST",
          headers: { "X-Queue-Id": ensureQueueId() }
        });
        if (!response.ok) {
          const error = await response.json();
          throw new Error(error.error || "Falha ao exportar Excel INT.");
        }
        const blob = await response.blob();
        const disposition = response.headers.get("Content-Disposition") || "";
        const match = disposition.match(/filename=\"([^\"]+)\"/i);
        const fileName = match ? match[1] : "lote-int.xlsx";
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = fileName;
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
        renderIntQueue(0, "Excel INT desta maquina exportado. O proximo lote ja comeca do zero.");
        await loadStats();
        scanInput.focus();
      } catch (error) {
        renderIntQueue(Number(intQueueCount.textContent || "0"), error.message);
      } finally {
        exportIntButton.disabled = false;
      }
    });

    openOtherPageButton.addEventListener("click", () => {
      window.location.href = "/demais-franquias";
    });

    async function loadStats() {
      const response = await fetch("/api/stats", {
        headers: { "X-Queue-Id": ensureQueueId() }
      });
      const data = await response.json();
      if (data.queue_id) {
        queueId = data.queue_id;
        localStorage.setItem(queueStorageKey, queueId);
      }
      document.querySelector("#statPedidos").textContent = data.pedidos ?? 0;
      document.querySelector("#statFranquias").textContent = data.franquias ?? 0;
      document.querySelector("#statAtualizado").textContent = shortDate(data.ultima_atualizacao);
      renderIntQueue(data.int_queue_count ?? 0, "Somente pedidos com unidade de entrega INT entram no lote desta maquina.");
      renderOtherQueue(data.other_queue_count ?? 0, "Todas as máquinas alimentam juntas este lote. A separação e exportação ficam na página própria.");

      document.querySelector("#importsBody").innerHTML = (data.imports || []).map(item => `
        <tr>
          <td>${esc(item.source_file)}</td>
          <td>${esc(item.inserted_rows)}</td>
          <td>${esc(item.updated_rows)}</td>
        </tr>
      `).join("");

      document.querySelector("#topBody").innerHTML = (data.top_franquias || []).map(item => `
        <tr>
          <td><span class="pill">${esc(item.franquia)}</span></td>
          <td>${esc(item.total)}</td>
        </tr>
      `).join("");
    }

    function updateClock() {
      document.querySelector("#clock").textContent = new Date().toLocaleString("pt-BR");
    }

    updateClock();
    setInterval(updateClock, 1000);
    loadStats();
    scanInput.focus();

    const syncButton = document.querySelector("#syncButton");
    const syncMessage = document.querySelector("#syncMessage");
    let syncPollTimer = null;

    async function checkSyncStatus() {
      try {
        const resp = await fetch("/api/sync-status");
        const data = await resp.json();
        if (data.status === "running") {
          syncButton.disabled = true;
          syncButton.textContent = "Sincronizando… ⟳";
          syncMessage.className = "message";
          syncMessage.textContent = data.message || "Sincronizando com TMS…";
          if (!syncPollTimer) syncPollTimer = setInterval(checkSyncStatus, 2000);
        } else {
          syncButton.disabled = false;
          syncButton.textContent = "Sincronizar com TMS";
          if (syncPollTimer) { clearInterval(syncPollTimer); syncPollTimer = null; }
          if (data.status === "done") {
            syncMessage.className = "message";
            syncMessage.textContent = data.message || "Sincronização concluída.";
            await loadStats();
          } else if (data.status === "error") {
            syncMessage.className = "message error";
            syncMessage.textContent = data.message || "Erro na sincronização.";
          }
        }
      } catch (e) {
        if (syncPollTimer) { clearInterval(syncPollTimer); syncPollTimer = null; }
        syncButton.disabled = false;
        syncButton.textContent = "Sincronizar com TMS";
      }
    }

    syncButton.addEventListener("click", async () => {
      syncButton.disabled = true;
      syncButton.textContent = "Sincronizando… ⟳";
      syncMessage.className = "message";
      syncMessage.textContent = "Iniciando sincronização com TMS…";
      try {
        await fetch("/api/sync-tms", { method: "POST" });
      } catch (e) {}
      syncPollTimer = setInterval(checkSyncStatus, 2000);
    });

    checkSyncStatus();
  </script>
</body>
</html>"""


def other_queue_page() -> str:
    return r"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Demais Franquias</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b1220;
      --bg-2: #111a2c;
      --surface: #121c2f;
      --surface-2: #182338;
      --surface-3: #0f1727;
      --text: #edf3ff;
      --muted: #96a4bc;
      --line: #263349;
      --line-strong: #31415d;
      --accent: #17b4a1;
      --accent-2: #129180;
      --danger: #ff7b72;
      --shadow: 0 18px 40px rgba(0, 0, 0, 0.28);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI Variable Text", "Segoe UI", Arial, Helvetica, sans-serif;
      line-height: 1.45;
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
      background:
        radial-gradient(circle at top, rgba(23, 180, 161, 0.08), transparent 32%),
        linear-gradient(180deg, var(--bg-2) 0%, var(--bg) 100%);
      color: var(--text);
    }
    header {
      background: rgba(8, 14, 24, 0.86); color: #fff; padding: 18px 28px; display: flex;
      align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap;
      backdrop-filter: blur(10px);
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    header a {
      color: #d8fff8;
      text-decoration: none;
      font-weight: 600;
      background: rgba(23, 180, 161, 0.12);
      border: 1px solid rgba(23, 180, 161, 0.24);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
    }
    h1, h2 { margin: 0; }
    h1 { font-size: 24px; }
    main { max-width: 1440px; margin: 0 auto; padding: 22px 18px 28px; display: grid; gap: 18px; }
    section {
      background: var(--surface);
      border: 1px solid rgba(255, 255, 255, 0.06);
      border-radius: 10px;
      padding: 20px;
      box-shadow: var(--shadow);
    }
    .toolbar { display: grid; grid-template-columns: minmax(220px, 280px) 1fr 220px; gap: 12px; align-items: end; }
    label {
      display: block;
      font-size: 12px;
      font-weight: 700;
      color: var(--muted);
      margin-bottom: 8px;
      text-transform: uppercase;
    }
    select, button {
      width: 100%;
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      padding: 12px 14px;
      font-size: 14px;
    }
    select {
      background: var(--surface-3);
      color: var(--text);
    }
    button {
      background: var(--accent);
      color: #fff;
      border: 0;
      font-weight: 700;
      cursor: pointer;
      box-shadow: 0 10px 22px rgba(23, 180, 161, 0.18);
      transition: transform 120ms ease, background 120ms ease, box-shadow 120ms ease;
    }
    button:hover { background: var(--accent-2); transform: translateY(-1px); }
    button:disabled { opacity: 0.58; cursor: wait; }
    .stats { display: grid; grid-template-columns: repeat(3, minmax(120px, 1fr)); gap: 10px; }
    .stat {
      background: var(--surface-2);
      border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 8px;
      padding: 14px;
    }
    .stat b { display: block; font-size: 26px; line-height: 1.1; color: #f5f8ff; }
    .stat span { color: var(--muted); font-size: 12px; font-weight: 700; }
    .message { margin-top: 14px; min-height: 22px; font-size: 13px; color: var(--muted); }
    .message.error { color: var(--danger); font-weight: 700; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { border-bottom: 1px solid rgba(255, 255, 255, 0.05); padding: 10px 8px; text-align: left; vertical-align: top; }
    th { color: var(--muted); font-size: 11px; background: rgba(255, 255, 255, 0.03); text-transform: uppercase; }
    tbody tr:hover { background: rgba(255, 255, 255, 0.025); }
    .pill {
      display: inline-block;
      padding: 4px 7px;
      border-radius: 999px;
      background: rgba(23, 180, 161, 0.14);
      color: #c7fff7;
      border: 1px solid rgba(23, 180, 161, 0.18);
      font-weight: 700;
    }
    .tables { display: grid; grid-template-columns: 320px 1fr; gap: 18px; }
    @media (max-width: 980px) {
      .toolbar, .tables, .stats { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Demais Franquias</h1>
    <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
      <a href="/">Voltar para bipagem</a>
      <div id="clock"></div>
    </div>
  </header>
  <main>
    <section>
      <div class="toolbar">
        <div>
          <label for="siglaFilter">Filtrar franquia</label>
          <select id="siglaFilter">
            <option value="">Todas as franquias</option>
          </select>
        </div>
        <div class="message" id="queueMessage">Este lote é compartilhado por toda a operação e só zera quando a franquia exportada for baixada.</div>
        <div>
          <button id="exportButton" type="button">Exportar franquia filtrada</button>
        </div>
      </div>
    </section>

    <section>
      <div class="stats">
        <div class="stat"><b id="statTotal">0</b><span>Total no lote geral</span></div>
        <div class="stat"><b id="statFiltered">0</b><span>Total da franquia filtrada</span></div>
        <div class="stat"><b id="statFranquias">0</b><span>Franquias no lote</span></div>
      </div>
    </section>

    <section class="tables">
      <div>
        <h2>Franquias pendentes</h2>
        <table>
          <thead>
            <tr><th>Sigla</th><th>Total</th></tr>
          </thead>
          <tbody id="summaryBody"></tbody>
        </table>
      </div>
      <div>
        <h2>Pedidos já bipados</h2>
        <table>
          <thead>
            <tr>
              <th>Hora</th>
              <th>Sigla</th>
              <th>Nro. Entrega</th>
              <th>Pedido</th>
              <th>Cliente</th>
            </tr>
          </thead>
          <tbody id="rowsBody"></tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    const siglaFilter = document.querySelector("#siglaFilter");
    const exportButton = document.querySelector("#exportButton");
    const queueMessage = document.querySelector("#queueMessage");
    const summaryBody = document.querySelector("#summaryBody");
    const rowsBody = document.querySelector("#rowsBody");

    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, char => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
      }[char]));
    }

    function shortDate(value) {
      if (!value) return "-";
      return String(value).replace("T", " ").slice(0, 19);
    }

    async function loadOtherQueue() {
      const sigla = siglaFilter.value;
      const response = await fetch(`/api/other-queue${sigla ? `?sigla=${encodeURIComponent(sigla)}` : ""}`);
      const data = await response.json();

      document.querySelector("#statTotal").textContent = data.total ?? 0;
      document.querySelector("#statFiltered").textContent = data.filtered_total ?? 0;
      document.querySelector("#statFranquias").textContent = (data.franquias || []).length;
      exportButton.disabled = !(data.filtered_total ?? 0);
      queueMessage.className = "message";
      queueMessage.textContent = data.selected_sigla
        ? `A exportação vai baixar e zerar a franquia ${data.selected_sigla} para toda a operação.`
        : "Escolha uma franquia para exportar só o lote daquela sigla.";

      const options = [`<option value="">Todas as franquias</option>`].concat(
        (data.franquias || []).map(item => `<option value="${esc(item.sigla)}" ${item.sigla === data.selected_sigla ? "selected" : ""}>${esc(item.sigla)} (${esc(item.total)})</option>`)
      );
      siglaFilter.innerHTML = options.join("");

      summaryBody.innerHTML = (data.franquias || []).map(item => `
        <tr>
          <td><span class="pill">${esc(item.sigla)}</span></td>
          <td>${esc(item.total)}</td>
        </tr>
      `).join("");

      rowsBody.innerHTML = (data.rows || []).map(item => `
        <tr>
          <td>${esc(shortDate(item.scanned_at))}</td>
          <td><span class="pill">${esc(item.sigla_unidade_entrega)}</span></td>
          <td>${esc(item.nro_entrega || "-")}</td>
          <td>${esc(item.pedido)}</td>
          <td>${esc(item.cliente || "-")}</td>
        </tr>
      `).join("");
    }

    siglaFilter.addEventListener("change", loadOtherQueue);

    exportButton.addEventListener("click", async () => {
      const sigla = siglaFilter.value;
      if (!sigla) {
        queueMessage.className = "message error";
        queueMessage.textContent = "Escolha uma franquia antes de exportar.";
        return;
      }
      exportButton.disabled = true;
      queueMessage.className = "message";
      queueMessage.textContent = `Gerando planilha da franquia ${sigla}...`;
      try {
        const response = await fetch("/api/export-other-workbook", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ sigla })
        });
        if (!response.ok) {
          const error = await response.json();
          throw new Error(error.error || "Falha ao exportar a franquia.");
        }
        const blob = await response.blob();
        const disposition = response.headers.get("Content-Disposition") || "";
        const match = disposition.match(/filename=\"([^\"]+)\"/i);
        const fileName = match ? match[1] : `lote-${sigla}.xlsx`;
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = fileName;
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
        queueMessage.textContent = `Franquia ${sigla} exportada. O lote dessa sigla foi zerado para todos.`;
        await loadOtherQueue();
      } catch (error) {
        queueMessage.className = "message error";
        queueMessage.textContent = error.message;
      } finally {
        exportButton.disabled = false;
      }
    });

    function updateClock() {
      document.querySelector("#clock").textContent = new Date().toLocaleString("pt-BR");
    }

    updateClock();
    setInterval(updateClock, 1000);
    loadOtherQueue();
  </script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    server_version = "FranquiaBipagem/2.0"

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write("%s - %s\n" % (self.log_date_time_string(), format % args))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            html_response(self, page())
            return

        if parsed.path == "/demais-franquias":
            html_response(self, other_queue_page())
            return

        if parsed.path == "/api/pedido":
            numero = parse_qs(parsed.query).get("numero", [""])[0]
            json_response(self, lookup_pedido(numero))
            return

        if parsed.path == "/api/stats":
            json_response(self, get_stats(self.headers.get("X-Queue-Id")))
            return

        if parsed.path == "/api/other-queue":
            sigla = parse_qs(parsed.query).get("sigla", [""])[0]
            json_response(self, get_other_queue_data(sigla))
            return

        if parsed.path == "/api/sync-status":
            json_response(self, dict(_sync_state))
            return

        html_response(self, "<h1>Pagina nao encontrada</h1>", HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/api/import":
            try:
                filename, data = parse_uploaded_file(self)
                json_response(self, import_csv_bytes(data, filename))
            except Exception as exc:
                json_response(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/scan-int":
            try:
                payload = parse_json_body(self)
                numero = clean_cell(payload.get("numero", ""))
                queue_id = payload.get("queue_id") or self.headers.get("X-Queue-Id")
                json_response(self, register_int_scan(numero, queue_id))
            except Exception as exc:
                json_response(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/scan-other":
            try:
                payload = parse_json_body(self)
                numero = clean_cell(payload.get("numero", ""))
                json_response(self, register_other_scan(numero))
            except Exception as exc:
                json_response(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/export-int-workbook":
            try:
                filename, body = export_int_workbook(self.headers.get("X-Queue-Id"))
                bytes_response(
                    self,
                    filename,
                    body,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            except ValueError as exc:
                json_response(self, {"error": str(exc)}, HTTPStatus.CONFLICT)
            except Exception as exc:
                json_response(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/export-other-workbook":
            try:
                payload = parse_json_body(self)
                sigla = clean_cell(payload.get("sigla", ""))
                filename, body = export_other_workbook(sigla)
                bytes_response(
                    self,
                    filename,
                    body,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            except ValueError as exc:
                json_response(self, {"error": str(exc)}, HTTPStatus.CONFLICT)
            except Exception as exc:
                json_response(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/sync-tms":
            if _sync_state["status"] == "running":
                json_response(self, {"ok": False, "message": "Sincronização já em andamento."})
            else:
                t = threading.Thread(target=_run_sync_job, args=(14,), daemon=True, name="tms-manual-sync")
                t.start()
                json_response(self, {"ok": True, "message": "Sincronização iniciada."})
            return

        json_response(self, {"error": "Rota POST nao suportada."}, HTTPStatus.METHOD_NOT_ALLOWED)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bipagem de franquia por numero de pedido")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8088, type=int)
    parser.add_argument("--import-csv", help="Importa um CSV e encerra")
    args = parser.parse_args()

    if args.import_csv:
        path = Path(args.import_csv)
        result = import_csv_bytes(path.read_bytes(), path.name)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    connect().close()
    _start_auto_sync()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Servidor iniciado em http://{args.host}:{args.port}")
    print("Pressione Ctrl+C para encerrar.")
    server.serve_forever()


if __name__ == "__main__":
    main()
