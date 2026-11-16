from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree


class FileParseError(ValueError):
    """A supported file exists but its contents cannot be parsed safely."""

    category = "file_parse_error"


class FileProcessingService:
    """Extract useful local document text without granting arbitrary filesystem access."""

    async def extract(self, path: Path, *, max_chars: int = 60_000) -> dict[str, Any]:
        return await asyncio.to_thread(self._extract_sync, path, max_chars)

    async def search(self, root: Path, query: str, *, limit: int = 30) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._search_sync, root, query, limit)

    def _search_sync(self, root: Path, query: str, limit: int) -> list[dict[str, Any]]:
        needle = query.strip().lower()
        if not needle:
            return []
        result: list[dict[str, Any]] = []
        allowed = {".txt", ".md", ".rst", ".log", ".json", ".yaml", ".yml", ".xml", ".html", ".htm", ".csv", ".pdf", ".docx", ".xlsx", ".xlsm"}
        # Dependency, build and VCS directories can contain hundreds of
        # thousands of files. They are not user-authored workspace documents
        # and scanning them was the main cause of unified-search timeouts.
        ignored_dirs = {".git", ".hg", ".svn", ".venv", "venv", "node_modules", "dist", "build", "__pycache__", ".koreagent-next", ".pytest_cache", ".ruff_cache"}
        scanned = 0
        for current, directories, filenames in os.walk(root, topdown=True, followlinks=False):
            directories[:] = sorted(name for name in directories if name not in ignored_dirs)
            for filename in sorted(filenames):
                if len(result) >= max(1, min(int(limit), 100)):
                    return result
                path = Path(current) / filename
                scanned += 1
                if scanned > 10_000:
                    return result
                if not path.is_file() or path.suffix.lower() not in allowed or path.is_symlink():
                    continue
                try:
                    if path.stat().st_size > 20 * 1024 * 1024:
                        continue
                except OSError:
                    continue
                try:
                    extracted = self._extract_sync(path, 30_000)
                except (OSError, ValueError, RuntimeError, zipfile.BadZipFile):
                    continue
                text = str(extracted.get("text") or "")
                if needle not in text.lower() and needle not in path.name.lower():
                    continue
                relative = str(path.relative_to(root))
                result.append(
                    {
                        "id": relative,
                        "type": "file",
                        "source": "workspace",
                        "title": path.name,
                        "path": relative,
                        "snippet": self._snippet(text, query),
                        "timestamp": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat().replace("+00:00", "Z"),
                        "metadata": extracted.get("metadata") or {},
                        "source_type": "workspace",
                        "entity_id": relative,
                        "detail_kind": "file",
                        "can_view_detail": True,
                        "can_open_source": True,
                        "source_ref": {"path": relative},
                    }
                )
        return result

    def _extract_sync(self, path: Path, max_chars: int) -> dict[str, Any]:
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size > 50 * 1024 * 1024:
            raise ValueError("file exceeds the 50 MiB extraction limit")
        suffix = path.suffix.lower()
        metadata = {"path": path.name, "extension": suffix, "size_bytes": path.stat().st_size}
        if suffix in {".txt", ".md", ".rst", ".log", ".json", ".yaml", ".yml", ".xml", ".html", ".htm", ".css", ".js", ".ts", ".tsx", ".py", ".csv"}:
            raw = path.read_text(encoding="utf-8-sig", errors="replace")
            if suffix == ".json":
                try:
                    raw = json.dumps(json.loads(raw), ensure_ascii=False, indent=2)
                except json.JSONDecodeError:
                    pass
            elif suffix == ".csv":
                raw = self._csv_text(raw)
            return self._result(metadata, raw, max_chars)
        if suffix == ".pdf":
            return self._extract_pdf(path, metadata, max_chars)
        if suffix == ".docx":
            return self._extract_docx(path, metadata, max_chars)
        if suffix in {".xlsx", ".xlsm"}:
            return self._extract_xlsx(path, metadata, max_chars)
        if suffix in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"}:
            try:
                from PIL import Image

                with Image.open(path) as image:
                    metadata.update({"width": image.width, "height": image.height, "format": image.format})
            except ImportError:
                pass
            return self._result(metadata, "这是一个图片文件。当前提供文件元数据；如需 OCR，请配置 OCR 服务。", max_chars)
        raise ValueError(f"unsupported file type: {suffix or 'ไม่มี扩展名'}")

    @staticmethod
    def _result(metadata: dict[str, Any], text: str, max_chars: int) -> dict[str, Any]:
        value = text[: max(1, min(int(max_chars), 200_000))]
        return {"metadata": metadata, "text": value, "truncated": len(text) > len(value), "characters": len(text)}

    @staticmethod
    def _snippet(content: str, query: str, width: int = 280) -> str:
        value = re.sub(r"\s+", " ", content).strip()
        if len(value) <= width:
            return value
        index = value.lower().find(query.lower())
        start = max(0, index - width // 3) if index >= 0 else 0
        return ("…" if start else "") + value[start : start + width] + ("…" if start + width < len(value) else "")

    @staticmethod
    def _csv_text(raw: str) -> str:
        rows = list(csv.reader(io.StringIO(raw)))
        return "\n".join(" | ".join(cell.strip() for cell in row) for row in rows)

    def _extract_pdf(self, path: Path, metadata: dict[str, Any], max_chars: int) -> dict[str, Any]:
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError("PDF extraction requires the pypdf package") from exc
        try:
            reader = PdfReader(str(path))
            metadata["pages"] = len(reader.pages)
            text = "\n\n".join((page.extract_text() or "") for page in reader.pages)
        except Exception as exc:
            raise FileParseError(f"file_parse_error: unable to parse PDF {path.name}") from exc
        return self._result(metadata, text, max_chars)

    def _extract_docx(self, path: Path, metadata: dict[str, Any], max_chars: int) -> dict[str, Any]:
        try:
            from docx import Document

            document = Document(str(path))
            text = "\n".join(p.text for p in document.paragraphs)
            metadata["paragraphs"] = len(document.paragraphs)
        except ImportError:
            try:
                with zipfile.ZipFile(path) as archive:
                    xml = archive.read("word/document.xml")
                root = ElementTree.fromstring(xml)
                text = "\n".join(t.text or "" for t in root.iter() if t.tag.endswith("}t"))
            except Exception as exc:
                raise FileParseError(f"file_parse_error: unable to parse Word document {path.name}") from exc
        except Exception as exc:
            raise FileParseError(f"file_parse_error: unable to parse Word document {path.name}") from exc
        return self._result(metadata, text, max_chars)

    def _extract_xlsx(self, path: Path, metadata: dict[str, Any], max_chars: int) -> dict[str, Any]:
        try:
            from openpyxl import load_workbook
        except ImportError as exc:
            raise RuntimeError("Excel extraction requires the openpyxl package") from exc
        try:
            book = load_workbook(str(path), read_only=True, data_only=True)
            lines: list[str] = []
            metadata["sheets"] = list(book.sheetnames)
            for sheet in book.worksheets:
                lines.append(f"[{sheet.title}]")
                for row in sheet.iter_rows(values_only=True):
                    values = ["" if value is None else str(value) for value in row]
                    if any(values):
                        lines.append(" | ".join(values))
            book.close()
        except Exception as exc:
            raise FileParseError(f"file_parse_error: unable to parse spreadsheet {path.name}") from exc
        return self._result(metadata, "\n".join(lines), max_chars)
