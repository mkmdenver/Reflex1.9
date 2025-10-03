# common/dbutils.py
"""
Database utilities for Reflex (PostgreSQL, psycopg v3).

- psycopg v3 only (no psycopg2).
- Prefers DB params from config helpers (common.config/config) and supports DB_PARAMS,
  then falls back to environment (REFLEX_DB_* then PG*).
- Optional .env via python-dotenv if present.
- Connection helpers (with optional pooling), transactions, query helpers.
- Bulk helpers and robust SQL statement splitter.

Env fallbacks:
  REFLEX_DB_HOST / PGHOST
  REFLEX_DB_PORT / PGPORT
  REFLEX_DB_NAME / PGDATABASE
  REFLEX_DB_USER / PGUSER
  REFLEX_DB_PASSWORD / PGPASSWORD
  REFLEX_DB_SSLMODE / PGSSLMODE
  REFLEX_DB_OPTIONS / PGOPTIONS
  REFLEX_DB_APP_NAME
  REFLEX_LOG_LEVEL
"""

from __future__ import annotations

import os
import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

# ---------------------- optional .env support ----------------------
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(override=False)
except Exception:
    pass

# ---------------------- psycopg v3 imports ------------------------
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.errors import Error as PsycopgError

# Optional pool: pip install psycopg_pool
try:
    from psycopg_pool import ConnectionPool  # type: ignore
    _HAVE_POOL = True
except Exception:
    ConnectionPool = None  # type: ignore
    _HAVE_POOL = False

Params = Optional[Union[Sequence[Any], Mapping[str, Any]]]

# ---------------------- logging -----------------------------------
def _build_logger() -> logging.Logger:
    try:
        # Prefer your own logger if available
        from common.app_logging import setup_logger  # type: ignore
        return setup_logger("db")
    except Exception:
        logger = logging.getLogger("db")
        if not logger.handlers:
            h = logging.StreamHandler()
            f = logging.Formatter("[%(asctime)s] %(levelname)s - %(name)s: %(message)s")
            h.setFormatter(f)
            logger.addHandler(h)
        level_name = os.getenv("REFLEX_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        logger.setLevel(level)
        return logger

logger = _build_logger()

# ---------------------- config helpers -----------------------------

def _lower_keys(d: Mapping[str, Any]) -> Dict[str, Any]:
    return {str(k).lower(): v for k, v in d.items()}

def _prefer_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Prefer REFLEX_DB_* then PG* for a given concept."""
    mapping = {
        "host": ("REFLEX_DB_HOST", "PGHOST"),
        "port": ("REFLEX_DB_PORT", "PGPORT"),
        "dbname": ("REFLEX_DB_NAME", "PGDATABASE"),
        "user": ("REFLEX_DB_USER", "PGUSER"),
        "password": ("REFLEX_DB_PASSWORD", "PGPASSWORD"),
        "sslmode": ("REFLEX_DB_SSLMODE", "PGSSLMODE"),
        "options": ("REFLEX_DB_OPTIONS", "PGOPTIONS"),
        "app_name": ("REFLEX_DB_APP_NAME", None),
    }
    envs = mapping.get(key, (None, None))
    for var in envs:
        if var and var in os.environ:
            return os.environ[var]
    return default

def _materialize_mapping_like(obj: Any) -> Optional[Mapping[str, Any]]:
    """Return a mapping view for mappings, dataclasses/objects with attributes, or None."""
    if obj is None:
        return None
    if isinstance(obj, Mapping):
        return obj
    # Try to read attributes
    attrs = ("host", "port", "dbname", "database", "name", "user", "username",
             "password", "pwd", "sslmode", "options")
    d: Dict[str, Any] = {}
    found = False
    for a in attrs:
        if hasattr(obj, a):
            d[a] = getattr(obj, a)
            found = True
    return d if found else None

def _find_helper_config() -> Tuple[Optional[Mapping[str, Any]], Optional[str]]:
    """
    Try to locate DB config/params in known helpers, in priority:
    - common.config: DB_PARAMS (preferred), get_db_config()/load_db_config(), DB_CONFIG/DB_SETTINGS
    - config:       DB_PARAMS, get_db_config()/load_db_config(), DB_CONFIG/DB_SETTINGS

    Returns: (mapping_like, dsn_override)  # dsn_override is used when helper provides a DSN/url
    """
    candidates = ("common.config", "config")
    getters = ("get_db_config", "load_db_config")
    constants = ("DB_PARAMS", "DB_CONFIG", "DB_SETTINGS")

    for mod_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=["*"])
        except Exception:
            continue

        # 1) DB_PARAMS (can be mapping, object, or hold DSN)
        if hasattr(mod, "DB_PARAMS"):
            dp = getattr(mod, "DB_PARAMS")
            # If it looks like a DSN-bearing object, prefer that DSN
            for ds_attr in ("to_dsn", "dsn", "url"):
                if hasattr(dp, ds_attr):
                    try:
                        dsn_val = dp.to_dsn() if ds_attr == "to_dsn" else getattr(dp, ds_attr)
                        if isinstance(dsn_val, str) and dsn_val.strip():
                            return None, dsn_val.strip()
                    except Exception:
                        logger.debug("DB_PARAMS.%s() failed; will try mapping fields.", ds_attr)
            # Otherwise treat as mapping-like
            mp = _materialize_mapping_like(dp)
            if mp:
                return mp, None

        # 2) Function getters
        for fn_name in getters:
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    cfg = fn()
                    # Maybe returns DSN
                    if isinstance(cfg, str):
                        return None, cfg.strip()
                    # Mapping-like
                    mp = _materialize_mapping_like(cfg) or (cfg.__dict__ if hasattr(cfg, "__dict__") else None)
                    if isinstance(mp, Mapping):
                        return mp, None
                except Exception:
                    logger.exception("Error calling %s.%s()", mod_name, fn_name)

        # 3) Constant mappings
        for attr in constants:
            if hasattr(mod, attr):
                cfg = getattr(mod, attr)
                if isinstance(cfg, str) and cfg.strip():
                    return None, cfg.strip()
                mp = _materialize_mapping_like(cfg) or (cfg.__dict__ if hasattr(cfg, "__dict__") else None)
                if isinstance(mp, Mapping):
                    return mp, None

    return None, None

def _load_db_config() -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Returns (normalized_config, dsn_override)
    normalized_config keys: host, port, dbname, user, password, sslmode?, options?
    dsn_override: a full DSN string from helpers, if provided.
    """
    helper_map, helper_dsn = _find_helper_config()
    if helper_map is None and helper_dsn is None:
        # Environment fallback
        helper_map = {
            "host": _prefer_env("host", "localhost"),
            "port": _prefer_env("port", "5432"),
            "dbname": _prefer_env("dbname", "TimeData"),
            "user": _prefer_env("user", ""),
            "password": _prefer_env("password", ""),
            "sslmode": _prefer_env("sslmode", "prefer"),
            "options": _prefer_env("options", None),
        }

    low = _lower_keys(helper_map or {})
    # normalize + aliases
    host = low.get("host") or low.get("hostname") or low.get("server")
    port = int(low.get("port", 5432) or 5432)
    dbname = low.get("dbname") or low.get("database") or low.get("name")
    user = low.get("user") or low.get("username")
    password = low.get("password") or low.get("pwd")
    sslmode = low.get("sslmode")
    options = low.get("options")

    if helper_dsn and not host and not dbname:
        # We have an authoritative DSN and no conflicting map; return minimal map
        return {"host": None, "port": None, "dbname": None, "user": None, "password": None, "sslmode": None, "options": None}, helper_dsn

    if not (host and dbname):
        raise RuntimeError("DB config incomplete: require host and dbname. (user/password may be empty for trust/local)")

    cfg = {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }
    if sslmode:
        cfg["sslmode"] = sslmode
    if options:
        cfg["options"] = options
    return cfg, helper_dsn

def _dsn_pair(k: str, v: Any) -> str:
    s = str(v)
    # quote if contains whitespace
    if any(ch.isspace() for ch in s):
        s = f"'{s}'"
    return f"{k}={s}"

def _append_param_to_dsn(dsn: str, k: str, v: Any) -> str:
    """Append/override a single libpq param to an existing DSN string."""
    if v is None or (isinstance(v, str) and not v.strip()):
        return dsn
    pair = _dsn_pair(k, v)
    # If key already present, keep original behavior simple: append (libpq last-one-wins)
    return f"{dsn} {pair}".strip()

def build_dsn(
    *,
    application_name: Optional[str] = None,
    connect_timeout: Optional[int] = None,
) -> str:
    """
    Build DSN string preferring config helpers (DB_PARAMS, etc.). If helpers
    provide a DSN, we use it and append application_name / connect_timeout if given.
    Otherwise, we build from normalized fields.
    """
    cfg, helper_dsn = _load_db_config()

    if helper_dsn:
        dsn = helper_dsn
        if application_name:
            dsn = _append_param_to_dsn(dsn, "application_name", application_name)
        if connect_timeout is not None:
            dsn = _append_param_to_dsn(dsn, "connect_timeout", int(connect_timeout))
        return dsn

    # Build from normalized fields
    parts = [
        _dsn_pair("host", cfg["host"]),
        _dsn_pair("port", cfg["port"]),
        _dsn_pair("dbname", cfg["dbname"]),
    ]
    if cfg.get("user"):
        parts.append(_dsn_pair("user", cfg["user"]))
    if cfg.get("password"):
        parts.append(_dsn_pair("password", cfg["password"]))
    if cfg.get("sslmode"):
        parts.append(_dsn_pair("sslmode", cfg["sslmode"]))
    if cfg.get("options"):
        parts.append(_dsn_pair("options", cfg["options"]))

    if application_name:
        parts.append(_dsn_pair("application_name", application_name))
    if connect_timeout is not None:
        parts.append(_dsn_pair("connect_timeout", int(connect_timeout)))
    return " ".join(parts)

# ---------------------- connection / pool -------------------------

_POOL: Optional["ConnectionPool"] = None  # type: ignore

def init_pool(
    dsn: Optional[str] = None,
    *,
    min_size: int = 1,
    max_size: int = 10,
    application_name: Optional[str] = None,
    connect_timeout: Optional[int] = None,
    conn_kwargs: Optional[Mapping[str, Any]] = None,
) -> None:
    """
    Initialize a global psycopg_pool pool.
    - Pulls DSN from config helpers unless explicit dsn is provided.
    - Validates pool sizes.
    """
    global _POOL

    if not _HAVE_POOL:
        raise ImportError("Connection pooling requires 'psycopg_pool'. Install with: pip install psycopg_pool")

    if min_size < 1 or max_size < 1 or min_size > max_size:
        raise ValueError(f"Invalid pool sizes: min_size={min_size}, max_size={max_size}")

    dsn_final = dsn or build_dsn(application_name=application_name, connect_timeout=connect_timeout)
    kwargs: Dict[str, Any] = dict(conn_kwargs or {})
    if connect_timeout is not None:
        kwargs.setdefault("connect_timeout", int(connect_timeout))

    _POOL = ConnectionPool(
        conninfo=dsn_final,
        min_size=min_size,
        max_size=max_size,
        kwargs=kwargs,
        # name="reflex-db-pool",
    )
    logger.info("Initialized connection pool (min=%s, max=%s)", min_size, max_size)

def get_pool() -> "ConnectionPool":
    if _POOL is None:
        raise RuntimeError("Pool not initialized. Call init_pool() first.")
    return _POOL

@contextmanager
def connection(
    *,
    dsn: Optional[str] = None,
    autocommit: bool = False,
    application_name: Optional[str] = None,
    connect_timeout: Optional[int] = None,
) -> Iterator[psycopg.Connection]:
    """
    Context manager yielding a psycopg connection.

    - If a pool is initialized and no explicit DSN is passed, borrows from the pool.
    - Otherwise, opens a direct connection and closes it on exit.
    """
    if _POOL is not None and dsn is None:
        # Pool-managed connection (closed/returned automatically)
        with _POOL.connection() as conn:  # type: ignore
            conn.autocommit = autocommit
            yield conn
        return

    # Fallback or explicit DSN: direct connection
    dsn_final = dsn or build_dsn(application_name=application_name, connect_timeout=connect_timeout)
    try:
        conn = psycopg.connect(dsn_final)
        conn.autocommit = autocommit
        try:
            yield conn
        finally:
            conn.close()
    except PsycopgError as e:
        logger.error("DB connect failed: %s", e)
        raise

def get_connection(
    dsn: Optional[str] = None,
    *,
    autocommit: bool = False,
    application_name: Optional[str] = None,
    connect_timeout: Optional[int] = None,
) -> psycopg.Connection:
    """
    Open a direct psycopg connection (non-pooled).
    Prefer the 'connection()' context manager for automatic cleanup.

    NOTE: If your app passes DB_PARAMS via helpers, this function now honors them.
    """
    dsn_final = dsn or build_dsn(application_name=application_name, connect_timeout=connect_timeout)
    try:
        conn = psycopg.connect(dsn_final)
        conn.autocommit = autocommit
        return conn
    except PsycopgError as e:
        logger.error("DB connect failed: %s", e)
        raise

@contextmanager
def transaction(
    *,
    dsn: Optional[str] = None,
    statement_timeout_ms: Optional[int] = None,
    application_name: Optional[str] = None,
    connect_timeout: Optional[int] = None,
) -> Iterator[psycopg.Connection]:
    """Open a transaction and commit/rollback automatically."""
    with connection(
        dsn=dsn,
        autocommit=False,
        application_name=application_name,
        connect_timeout=connect_timeout,
    ) as conn:
        try:
            if statement_timeout_ms is not None:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = %s", (int(statement_timeout_ms),))
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

# ---------------------- query helpers -----------------------------

def execute(
    sql_text: Union[str, sql.SQL],
    params: Params = None,
    *,
    dsn: Optional[str] = None,
    commit: bool = True,
) -> int:
    """Execute non-SELECT; return rowcount or -1."""
    with connection(dsn=dsn, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(sql_text, params)
                if commit:
                    conn.commit()
                return cur.rowcount if cur.rowcount is not None else -1
        except Exception:
            conn.rollback()
            raise

def execute_many(
    sql_text: Union[str, sql.SQL],
    seq_of_params: Iterable[Sequence[Any]],
    *,
    dsn: Optional[str] = None,
    commit: bool = True,
) -> int:
    total = 0
    with connection(dsn=dsn, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.executemany(sql_text, seq_of_params)
                total = cur.rowcount if cur.rowcount is not None else -1
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return total

def query_one(
    sql_text: Union[str, sql.SQL],
    params: Params = None,
    *,
    dsn: Optional[str] = None,
    as_dict: bool = True,
) -> Optional[Union[Mapping[str, Any], Tuple[Any, ...]]]:
    with connection(dsn=dsn, autocommit=True) as conn:
        rf = dict_row if as_dict else None
        with conn.cursor(row_factory=rf) as cur:
            cur.execute(sql_text, params)
            return cur.fetchone()

def query_all(
    sql_text: Union[str, sql.SQL],
    params: Params = None,
    *,
    dsn: Optional[str] = None,
    as_dict: bool = True,
) -> List[Union[Mapping[str, Any], Tuple[Any, ...]]]:
    with connection(dsn=dsn, autocommit=True) as conn:
        rf = dict_row if as_dict else None
        with conn.cursor(row_factory=rf) as cur:
            cur.execute(sql_text, params)
            return cur.fetchall()

# ---------------------- bulk helpers ------------------------------

def _ident_qualified(name: str) -> sql.Composable:
    parts = [p.strip() for p in name.split(".") if p.strip()]
    if not parts:
        raise ValueError("Empty identifier")
    comp: sql.Composable = sql.Identifier(parts[0])
    for p in parts[1:]:
        comp = sql.Composed([comp, sql.SQL("."), sql.Identifier(p)])
    return comp

def _ensure_rows_and_columns(
    rows_or_columns: Union[Iterable[Mapping[str, Any]], Sequence[str]],
    maybe_rows: Optional[Iterable[Mapping[str, Any]]] = None,
) -> Tuple[List[str], List[Mapping[str, Any]]]:
    if isinstance(rows_or_columns, (list, tuple)) and rows_or_columns and isinstance(rows_or_columns[0], str):
        if maybe_rows is None:
            raise ValueError("rows must be provided when columns are specified explicitly.")
        columns: List[str] = list(rows_or_columns)  # type: ignore
        rows: List[Mapping[str, Any]] = list(maybe_rows)
        if not rows:
            return columns, []
    else:
        rows = list(rows_or_columns)  # type: ignore
        if not rows:
            raise ValueError("rows collection is empty.")
        if not isinstance(rows[0], Mapping):
            raise TypeError("rows must be mapping objects.")
        columns = list(rows[0].keys())
    # Validate completeness
    for i, r in enumerate(rows):
        missing = [c for c in columns if c not in r]
        if missing:
            raise ValueError(f"Row {i} missing columns: {missing}")
    return columns, rows

def bulk_insert(
    table: str,
    rows_or_columns: Union[Iterable[Mapping[str, Any]], Sequence[str]],
    rows: Optional[Iterable[Mapping[str, Any]]] = None,
    *,
    dsn: Optional[str] = None,
    chunk_size: int = 1000,
    commit: bool = True,
) -> int:
    columns, row_dicts = _ensure_rows_and_columns(rows_or_columns, rows)
    if not row_dicts:
        return 0
    table_ident = _ident_qualified(table)
    col_idents = [sql.Identifier(c) for c in columns]
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))
    stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(table_ident, sql.SQL(", ").join(col_idents), placeholders)

    total = 0
    with connection(dsn=dsn, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                batch: List[Sequence[Any]] = []
                for r in row_dicts:
                    batch.append(tuple(r[c] for c in columns))
                    if len(batch) >= chunk_size:
                        cur.executemany(stmt, batch)
                        total += cur.rowcount if cur.rowcount is not None else len(batch)
                        batch.clear()
                if batch:
                    cur.executemany(stmt, batch)
                    total += cur.rowcount if cur.rowcount is not None else len(batch)
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return total

def bulk_upsert(
    table: str,
    rows_or_columns: Union[Iterable[Mapping[str, Any]], Sequence[str]],
    rows: Optional[Iterable[Mapping[str, Any]]] = None,
    *,
    conflict_cols: Sequence[str],
    update_cols: Optional[Sequence[str]] = None,
    dsn: Optional[str] = None,
    chunk_size: int = 1000,
    commit: bool = True,
) -> int:
    if not conflict_cols:
        raise ValueError("conflict_cols must be provided.")
    columns, row_dicts = _ensure_rows_and_columns(rows_or_columns, rows)
    if not row_dicts:
        return 0
    if update_cols is None:
        update_cols = [c for c in columns if c not in conflict_cols]
    do_nothing = len(update_cols) == 0

    table_ident = _ident_qualified(table)
    col_idents = [sql.Identifier(c) for c in columns]
    conflict_idents = [sql.Identifier(c) for c in conflict_cols]
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))

    if do_nothing:
        stmt = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
        ).format(table_ident, sql.SQL(", ").join(col_idents), placeholders, sql.SQL(", ").join(conflict_idents))
    else:
        set_exprs = [
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in (update_cols or [])
        ]
        stmt = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
            table_ident, sql.SQL(", ").join(col_idents), placeholders,
            sql.SQL(", ").join(conflict_idents), sql.SQL(", ").join(set_exprs)
        )

    total = 0
    with connection(dsn=dsn, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                batch: List[Sequence[Any]] = []
                for r in row_dicts:
                    batch.append(tuple(r[c] for c in columns))
                    if len(batch) >= chunk_size:
                        cur.executemany(stmt, batch)
                        total += cur.rowcount if cur.rowcount is not None else len(batch)
                        batch.clear()
                if batch:
                    cur.executemany(stmt, batch)
                    total += cur.rowcount if cur.rowcount is not None else len(batch)
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return total

# ---------------------- SQL scripts --------------------------------

def _split_sql_statements(sql_text: str) -> List[str]:
    """
    Split SQL text into statements by semicolons, respecting:
    - single/double quotes
    - dollar-quoted blocks ($tag$...$tag$)
    - line comments (--) and block comments (/*...*/)
    """
    stmts: List[str] = []
    buf: List[str] = []

    in_squote = False
    in_dquote = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: Optional[str] = None

    i = 0
    n = len(sql_text)

    def at(idx: int) -> str:
        return sql_text[idx] if 0 <= idx < n else ""

    while i < n:
        ch = sql_text[i]
        nxt = at(i + 1)

        # finish line comment
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # finish block comment
        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                i += 2
                in_block_comment = False
            else:
                i += 1
            continue

        # start comments when not inside quotes/dollar
        if not in_squote and not in_dquote and dollar_tag is None:
            if ch == "-" and nxt == "-":
                buf.append(ch); buf.append(nxt); i += 2; in_line_comment = True; continue
            if ch == "/" and nxt == "*":
                buf.append(ch); buf.append(nxt); i += 2; in_block_comment = True; continue

        # dollar-quoted block start?
        if not in_squote and not in_dquote and ch == "$":
            j = i + 1
            while j < n and sql_text[j] != "$" and (sql_text[j].isalnum() or sql_text[j] == "_"):
                j += 1
            if j < n and sql_text[j] == "$":
                dollar_tag = sql_text[i + 1 : j]  # may be ""
                buf.append(sql_text[i : j + 1])
                i = j + 1
                continue

        # dollar-quoted block end?
        if dollar_tag is not None:
            if ch == "$":
                j = i + 1
                k = j
                while k < n and sql_text[k] != "$" and (sql_text[k].isalnum() or sql_text[k] == "_"):
                    k += 1
                if k < n and sql_text[k] == "$":
                    tag = sql_text[j:k]
                    if tag == dollar_tag:
                        buf.append(sql_text[i : k + 1])
                        i = k + 1
                        dollar_tag = None
                        continue
            buf.append(ch); i += 1; continue

        # quotes
        if ch == "'" and not in_dquote:
            in_squote = not in_squote; buf.append(ch); i += 1; continue
        if ch == '"' and not in_squote:
            in_dquote = not in_dquote; buf.append(ch); i += 1; continue

        # statement boundary
        if ch == ";" and not in_squote and not in_dquote and dollar_tag is None:
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf.clear()
            i += 1
            continue

        buf.append(ch); i += 1

    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts

def execute_sql_commands(
    sql_commands: Union[str, Sequence[str]],
    *,
    dsn: Optional[str] = None,
    stop_on_error: bool = True,
    autocommit: bool = False,
    echo: bool = False,
) -> None:
    """
    Execute one or more SQL commands.
    - If a string is provided, it may contain multiple statements; safe-split is applied.
    - If stop_on_error is True (default), all statements run in a single transaction.
    - If stop_on_error is False, statements are executed independently (autocommit forced True).
    - echo=True logs each statement beforehand.
    """
    if isinstance(sql_commands, str):
        stmts = _split_sql_statements(sql_commands)
    else:
        stmts = [s.strip() for s in sql_commands if isinstance(s, str)]
    if not stmts:
        return

    conn_autocommit = True if not stop_on_error else autocommit
    with connection(dsn=dsn, autocommit=conn_autocommit) as conn:
        if stop_on_error:
            try:
                with conn.cursor() as cur:
                    for s in stmts:
                        if not s:
                            continue
                        if echo:
                            logger.info("[SQL] %s", s)
                        cur.execute(s)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        else:
            with conn.cursor() as cur:
                for s in stmts:
                    if not s:
                        continue
                    try:
                        if echo:
                            logger.info("[SQL] %s", s)
                        cur.execute(s)
                    except Exception as e:
                        logger.error("SQL error (continued): %s\nStatement: %s", e, s)

def execute_sql_file(
    path: str,
    *,
    dsn: Optional[str] = None,
    stop_on_error: bool = True,
    autocommit: bool = False,
    encoding: str = "utf-8",
    echo: bool = False,
) -> None:
    """Read a .sql file and execute its statements."""
    with open(path, "r", encoding=encoding) as f:
        sql_text = f.read()
    execute_sql_commands(
        sql_text,
        dsn=dsn,
        stop_on_error=stop_on_error,
        autocommit=autocommit,
        echo=echo,
    )

# ---------------------- utilities ---------------------------------

def set_search_path(path: Sequence[str], *, dsn: Optional[str] = None) -> None:
    identifiers = sql.SQL(",").join(sql.Identifier(p) for p in path)
    stmt = sql.SQL("SET search_path TO {}").format(identifiers)
    execute(stmt, dsn=dsn, commit=True)

def health_check(*, dsn: Optional[str] = None) -> bool:
    try:
        r = query_one("SELECT 1 AS ok", dsn=dsn, as_dict=True)
        return bool(r and (r.get("ok") == 1 or r.get("ok") == 1.0))
    except Exception as e:
        logger.warning("DB health_check failed: %s", e)
        return False

# Convenience helpers that operate on an existing connection --------------------

def exec_sql_query(conn: psycopg.Connection, sql_text: str, logger_: Optional[logging.Logger] = None) -> List[Dict[str, Any]]:
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql_text)
            rows = cur.fetchall()
            if logger_:
                logger_.info("[📥] Retrieved %d rows.", len(rows))
            return [dict(row) for row in rows]
    except Exception as e:
        if logger_:
            logger_.error("[❌] Query failed: %s", e)
        return []

def execute_sql_list_on_connection(sql_list: List[str], conn: psycopg.Connection, logger_: Optional[logging.Logger] = None) -> None:
    """
    Execute a list of SQL statements against an existing connection (autocommit on).
    """
    conn.autocommit = True
    with conn.cursor() as cur:
        for s in sql_list:
            preview = s.strip().splitlines()[0] if s.strip() else "[Empty SQL]"
            try:
                cur.execute(s)
                msg = f"✅ Executed: {preview}"
                if logger_:
                    logger_.info(msg)
                else:
                    print(msg)
            except Exception as e:
                msg = f"[❌] Failed: {preview}\n{e}"
                if logger_:
                    logger_.error(msg)
                else:
                    print(msg)

__all__ = [
    # Core config
    "build_dsn",

    # Connection / pool
    "get_connection", "connection", "transaction", "init_pool", "get_pool",

    # Query helpers
    "execute", "execute_many", "query_one", "query_all",

    # Bulk helpers
    "bulk_insert", "bulk_upsert",

    # SQL scripts
    "execute_sql_commands", "execute_sql_file",

    # Utilities
    "set_search_path", "health_check",

    # On-connection utilities
    "exec_sql_query", "execute_sql_list_on_connection",
]