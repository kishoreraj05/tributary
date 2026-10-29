"""The part of a document one node's preview needs.

Testing a node must not wait for the rest of the canvas to be finished: a half-configured
node downstream is not the tested node's problem. So a preview compiles only the node,
everything upstream of it, and the quality blocks attached to those; the other nodes are
left out of the document before it is validated. Nothing here validates anything: the
result is compiled like any document, and the issues reported are those of the nodes that
remain, which is exactly the set that can affect the preview.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

PREVIEW_SINK_ID = "__preview__"

_SECTIONS = ("sources", "transforms", "quality", "sinks")


def _entries(doc: Mapping[str, Any], section: str) -> list[dict[str, Any]]:
    return [n for n in (doc.get(section) or []) if isinstance(n, dict) and isinstance(n.get("id"), str)]


def _input_ids(node: dict[str, Any], section: str) -> list[str]:
    """What a node reads, as written (transform inputs may be ids or {node: id})."""
    if section == "transforms":
        inputs = node.get("inputs") if isinstance(node.get("inputs"), list) else []
        return [i if isinstance(i, str) else i.get("node") for i in inputs if isinstance(i, (str, dict))]
    if section == "sinks":
        return [node["input"]] if isinstance(node.get("input"), str) else []
    if section == "quality":
        return [node[k] for k in ("attach_to", "before") if isinstance(node.get(k), str)]
    return []


def restrict_to(doc: Mapping[str, Any], node_id: str) -> dict[str, Any]:
    """``doc`` with only ``node_id``, its upstream nodes and the quality blocks attached to
    them. Unknown ids are left alone (the document is returned as is, and the usual
    validation reports whatever is wrong). Since a spec needs a sink and previews never
    write, a stand-in console sink is added when none of the kept nodes is one."""
    doc = copy.deepcopy(dict(doc))
    nodes: dict[str, tuple[str, dict[str, Any]]] = {}
    for section in _SECTIONS:
        for node in _entries(doc, section):
            nodes.setdefault(node["id"], (section, node))  # duplicates are reported by validation
    if node_id not in nodes:
        return doc

    keep: set[str] = set()
    stack = [node_id]
    while stack:
        nid = stack.pop()
        if nid in keep or nid not in nodes:
            continue
        keep.add(nid)
        stack.extend(i for i in _input_ids(nodes[nid][1], nodes[nid][0]) if isinstance(i, str))
    # Gates attached to a kept node sit between it and its consumers (and a node's preview
    # shows the results of the gates attached to it), so they come along.
    for section, node in nodes.values():
        if section == "quality" and any(i in keep for i in _input_ids(node, "quality")):
            keep.add(node["id"])

    for section in _SECTIONS:
        if isinstance(doc.get(section), list):
            doc[section] = [n for n in doc[section] if isinstance(n, dict) and n.get("id") in keep]
    if not any(n["id"] in keep for n in _entries(doc, "sinks")):
        section, node = nodes[node_id]
        read = node_id if section in ("sources", "transforms") else next(iter(_input_ids(node, section)), node_id)
        doc["sinks"] = [*(doc.get("sinks") or []), {"id": PREVIEW_SINK_ID, "type": "console", "input": read}]
    return doc
