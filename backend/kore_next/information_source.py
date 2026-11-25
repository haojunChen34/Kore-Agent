"""Revalidate a derived suggestion against the exact synchronized source."""
from typing import Any

from .errors import RpcFault
from .external_information import fingerprint
from .util import loads


async def validate_information_source(db: Any, information_id: str, expected: str | None = None) -> dict[str, Any]:
    row = await (await db.execute("SELECT * FROM external_information WHERE id=?", (information_id,))).fetchone()
    source = await (await db.execute("SELECT * FROM connector_items WHERE id=?", (row["connector_item_id"] if row else None,))).fetchone()
    if not row or not source or source["status"] != "active":
        raise RpcFault(-32001, "原始来源已不可用，请重新同步并核实；该来源不能用于执行外部操作。")
    item = loads(source["data_json"], {})
    if source["provider"] != row["provider"] or source["source_id"] != row["source_id"]:
        raise RpcFault(-32001, "来源身份不一致，请重新同步并分析。")
    current = fingerprint(item)
    if current != row["content_fingerprint"] or (expected is not None and current != expected):
        raise RpcFault(-32001, "原始来源已变化，请重新分析并确认最新建议。")
    return item
