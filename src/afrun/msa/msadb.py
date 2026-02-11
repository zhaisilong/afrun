#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# MIT License
# Author: Silong Zhai
# Update: 2025-12-26

from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import sqlite3
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional
from loguru import logger
from pathlib import Path
from tqdm.auto import tqdm
from astra.data.sequence import sha256_hash

DEFAULT_DB_DIR = Path("~/.astra/msa").expanduser()
if not DEFAULT_DB_DIR.exists():
    logger.info(
        f"The MSA database directory does not exist. Creating it: {DEFAULT_DB_DIR}"
    )
    DEFAULT_DB_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_DB_PATH = DEFAULT_DB_DIR / "msa.sqlite"


# ==========
# 数据模型
# ----------
@dataclass
class MSA:
    """
    极简 MSA 数据对象。
    - sequence: 必填，字符串
    - unpairedMsa / pairedMsa: 可选字符串
    - templates: 可选 List[str]（存入 payload 时为 JSON 数组）
    - id: 可选，但会在 upsert 时被强制覆盖为 sha256(sequence)
    """

    sequence: str
    unpairedMsa: Optional[str] = None
    pairedMsa: Optional[str] = None
    templates: Optional[List[str]] = None
    id: Optional[str] = None  # 最终会被计算后的 sha256 覆盖

    def validate(self) -> None:
        if not isinstance(self.sequence, str) or not self.sequence.strip():
            raise ValueError("MSA.sequence 必须为非空字符串")

    def to_payload(self) -> Dict[str, Any]:
        """序列化为 payload（入库前将被 gzip 压缩）"""
        return {
            "id": self.id,  # 注意：最终会是 sha256(sequence)
            "sequence": self.sequence,
            "unpairedMsa": self.unpairedMsa,
            "pairedMsa": self.pairedMsa,
            "templates": self.templates,
        }

    def __repr__(self) -> str:
        return f"MSA|id={self.id}|sequence={self.sequence}"

    def __str__(self) -> str:
        return self.__repr__()


# ==========
# 工具函数
# ----------
class MSAUtils:
    @staticmethod
    def now_utc_iso() -> str:
        return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    @staticmethod
    def gzip_compress(obj: Dict[str, Any]) -> bytes:
        raw = json.dumps(
            obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        return gzip.compress(raw)

    @staticmethod
    def gzip_decompress(blob: bytes) -> Dict[str, Any]:
        raw = gzip.decompress(blob)
        return json.loads(raw.decode("utf-8"))


# ==========
# SQLite 存储层（仅 3 列）
# ----------
class MSADB:
    def __init__(self, db_path: str | None = None):
        if db_path is None:
            db_path = DEFAULT_DB_PATH
            
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._ensure_schema()

    def __repr__(self) -> str:
        return f"MSADB|db_path={self.db_path}"

    def __str__(self) -> str:
        return self.__repr__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self.conn.close()

    def _ensure_schema(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS msa (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload BLOB NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_msa_updated_at ON msa(updated_at);")
        self.conn.commit()

    # ---- 基本 CRUD ----
    def upsert_msa(self, msa: MSA) -> str:
        """
        新增或覆盖（按 id=sha256(sequence)）
        - 忽略传入的 msa.id；一律以 sequence 计算 hash 作为最终 id
        - 如果已有同 id：更新 updated_at 与 payload（覆盖）
        """
        msa.validate()
        computed_id = sha256_hash(msa.sequence)
        msa.id = computed_id  # 覆盖为最终 id
        now = MSAUtils.now_utc_iso()
        blob = MSAUtils.gzip_compress(msa.to_payload())

        self.conn.execute(
            """
            INSERT INTO msa (id, created_at, updated_at, payload)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                updated_at=excluded.updated_at,
                payload=excluded.payload;
            """,
            (computed_id, now, now, blob),
        )
        self.conn.commit()
        # 失效缓存
        self.get_header.cache_clear()  # type: ignore
        self.get_msa.cache_clear()  # type: ignore
        return computed_id

    @lru_cache(maxsize=1024)
    def get_header(self, id_: str) -> Optional[Dict[str, Any]]:
        """返回基本头信息（不解压 payload）"""
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id, created_at, updated_at FROM msa WHERE id=?;",
            (id_,),
        )
        row = cur.fetchone()
        if not row:
            return None
        (rid, created_at, updated_at) = row
        return {"id": rid, "created_at": created_at, "updated_at": updated_at}

    @lru_cache(maxsize=256)
    def get_msa(self, id_: str) -> Optional[Dict[str, Any]]:
        """返回解压后的完整 payload（JSON 字典）"""
        cur = self.conn.cursor()
        cur.execute("SELECT payload FROM msa WHERE id=?;", (id_,))
        row = cur.fetchone()
        if not row:
            return None
        return MSAUtils.gzip_decompress(row[0])

    def delete(self, id_: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM msa WHERE id=?;", (id_,))
        self.conn.commit()
        self.get_header.cache_clear()  # type: ignore
        self.get_msa.cache_clear()  # type: ignore
        return cur.rowcount > 0

    def list(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """按更新时间倒序列出条目（仅头部信息）"""
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, created_at, updated_at
            FROM msa
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?;
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        out = []
        for rid, created_at, updated_at in rows:
            out.append({"id": rid, "created_at": created_at, "updated_at": updated_at})
        return out

    def search(self, q: str, limit: int = 50) -> List[str]:
        """
        仅在 id 上做 LIKE 检索（不区分大小写）
        返回匹配到的 id 列表（按更新时间倒序）
        """
        if not q:
            return []
        qnorm = f"%{q.lower()}%"
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id FROM msa
            WHERE LOWER(id) LIKE ?
            ORDER BY updated_at DESC
            LIMIT ?;
            """,
            (qnorm, limit),
        )
        return [r[0] for r in cur.fetchall()]

    def import_json_file(self, path: str, force: bool = False) -> str:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        sequences = data.get("sequences")
        if not sequences:
            raise ValueError(f"The JSON file {path} does not contain any sequences")

        imported_list = []

        for sequence in sequences:
            protein = sequence.get("protein")

            if not protein:
                logger.warning(f"{sequence} is not a protein")
                continue
            protein_id = protein.get("id")

            protein_sequence = protein.get("sequence")
            unpaired_msa = protein.get("unpairedMsa")
            paired_msa = protein.get("pairedMsa")
            templates = protein.get("templates")

            if not protein_sequence:
                raise ValueError(
                    f"The JSON file {path} @ {protein_id} | {protein_sequence} does not contain any protein sequence"
                )

            if not force:
                if self.get_header(sha256_hash(protein_sequence)):
                    logger.warning(
                        f"The protein {sha256_hash(protein_sequence)} | {protein_sequence} already exists in the database, please use --force to overwrite"
                    )
                    continue

            if not unpaired_msa or not paired_msa or not templates:
                logger.warning(
                    f"The JSON file {path} @ {protein_id} | {protein_sequence} does not contain any unpaired MSA or paired MSA or templates"
                )
            msa = MSA(
                sequence=protein_sequence,
                unpairedMsa=unpaired_msa,
                pairedMsa=paired_msa,
                templates=templates,
            )
            msa_id = self.upsert_msa(msa)
            imported_list.append(msa_id)

        return imported_list

    def import_json_files(self, paths: Iterable[str]) -> List[str]:
        ids: List = []
        for p in tqdm(paths, desc="Importing JSON files"):
            ids.extend(self.import_json_file(p))
        return ids

    def update_json_file(self, path: Path) -> str:
        if not path.exists() or not path.is_file():
            raise ValueError(f"The JSON file {path} does not exist")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        updated_sequences = []
        for sequence in data["sequences"]:
            protein = sequence.get("protein")
            if not protein:
                logger.warning(f"[SKIP] {sequence} is not a protein")
                updated_sequences.append(sequence)
                continue

            protein_id = protein.get("id")
            protein_sequence = protein.get("sequence")
            unpaired_msa = protein.get("unpairedMsa")
            paired_msa = protein.get("pairedMsa")
            templates = protein.get("templates")
            if unpaired_msa and paired_msa and templates:
                logger.warning(
                    "Already have unpaired MSA, paired MSA, and templates, this will not be updated"
                )

            query_id = sha256_hash(protein_sequence)
            search_result = self.search(query_id)
            if not search_result:
                logger.warning(
                    f"The protein {query_id} | {protein_sequence} not found in the database, please import it first"
                )
            else:
                logger.info(
                    f"The protein {query_id} | {protein_sequence} found in the database, updating it"
                )
                id_ = search_result[0]
                msa = self.get_msa(id_)

            updated_sequences.append(
                {
                    "protein": {
                        "id": protein_id,
                        "sequence": protein_sequence,
                        "unpairedMsa": (
                            unpaired_msa if unpaired_msa else msa["unpairedMsa"]
                        ),
                        "pairedMsa": paired_msa if paired_msa else msa["pairedMsa"],
                        "templates": templates if templates else msa["templates"],
                    }
                }
            )
        data["sequences"] = updated_sequences
        return data

    # ---- 导入导出 ----
    # def import_json_files(self, paths: Iterable[str]) -> List[str]:
    #     """
    #     从 JSON 文件导入：
    #       - 支持单条对象：{"sequence": "...", "unpairedMsa": "...", "pairedMsa": "...", "templates":[...]}
    #       - 或对象数组：[ {...}, {...}, ... ]
    #     导入时自动计算 id=sha256(sequence) 并 upsert
    #     返回导入/更新的 id 列表
    #     """
    #     ids: List[str] = []
    #     for p in paths:
    #         with open(p, "r", encoding="utf-8") as f:
    #             data = json.load(f)
    #         if isinstance(data, list):
    #             for obj in data:
    #                 msa = self._msa_from_any(obj)
    #                 ids.append(self.upsert_msa(msa))
    #         else:
    #             msa = self._msa_from_any(data)
    #             ids.append(self.upsert_msa(msa))
    #     return ids

    # def export_json_files(
    #     self, out_dir: str, ids: Optional[Iterable[str]] = None
    # ) -> List[str]:
    #     """
    #     将库内 payload 解压为 JSON 文件：
    #       - 默认导出全部；如提供 ids 则导出指定条目
    #       - 文件名：<id>.msa.json
    #     返回导出文件路径列表
    #     """
    #     os.makedirs(out_dir, exist_ok=True)
    #     paths: List[str] = []
    #     cur = self.conn.cursor()
    #     if ids:
    #         ids = list(ids)
    #         if len(ids) == 0:
    #             return paths
    #         qmarks = ",".join(["?"] * len(ids))
    #         cur.execute(
    #             f"SELECT id, payload FROM msa WHERE id IN ({qmarks});", tuple(ids)
    #         )
    #     else:
    #         cur.execute("SELECT id, payload FROM msa;")

    #     for rid, blob in cur.fetchall():
    #         payload = MSAUtils.gzip_decompress(blob)
    #         fp = os.path.join(out_dir, f"{rid}.msa.json")
    #         with open(fp, "w", encoding="utf-8") as f:
    #             json.dump(payload, f, ensure_ascii=False, indent=2)
    #         paths.append(fp)
    #     return paths

    # ---- 内部：容错映射 ----
    # def _msa_from_any(self, obj: Dict[str, Any]) -> MSA:
    #     """
    #     将任意 dict 映射为 MSA：
    #       - sequence: 必须存在且为字符串（若不存在则 ValueError）
    #       - 其余字段可选
    #       - 若给了 id，也会被忽略（最终以 sequence 计算）
    #     """
    #     sequence = obj.get("sequence")
    #     if not isinstance(sequence, str) or not sequence.strip():
    #         raise ValueError(f"导入的对象缺少有效的 'sequence' 字段（非空字符串）")

    #     unpaired = obj.get("unpairedMsa")
    #     paired = obj.get("pairedMsa")
    #     templates = obj.get("templates")

    #     # 规范化 templates：若给到非 list[str]，尽量转换
    #     if templates is not None:
    #         if isinstance(templates, list):
    #             templates = [str(x) for x in templates]

    #     return MSA(
    #         sequence=sequence,
    #         unpairedMsa=(
    #             unpaired
    #             if (isinstance(unpaired, str) or unpaired is None)
    #             else str(unpaired)
    #         ),
    #         pairedMsa=(
    #             paired if (isinstance(paired, str) or paired is None) else str(paired)
    #         ),
    #         templates=templates,
    #         id=None,  # 会在 upsert_msa 中被 sequence 的哈希覆盖
    #     )

    def _file_size_human_readable(self) -> str:
        size = os.path.getsize(self.db_path)  # bytes
        # 把 bytes 转成人类友好格式
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"

    def status(self) -> Dict[str, Any]:
        return {
            "msa_db": str(self.db_path),
            "msa_count": self.count(),
            "msa_size": self._file_size_human_readable(),
        }

    def count(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM msa;")
        return cur.fetchone()[0]

    def size(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT SUM(LENGTH(payload)) FROM msa;")


def open_db(path: str) -> MSADB:
    return MSADB(path)


def seq2json(
    sequence: str, msa_db: MSADB, id: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Convert a sequence to a JSON object"""
    try:
        msa_id = msa_db.search(sha256_hash(sequence))[0]
        msa = msa_db.get_msa(msa_id)
        if id:
            msa["id"] = id
        return msa
    except Exception as e:
        logger.error(f"Error converting sequence to JSON: {e}")
        return None
