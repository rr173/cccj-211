# -*- coding: utf-8 -*-
"""策略声明：允许的层、标签、关系类型的端点层约束与最少支持团队数。

策略 JSON 示例::

    {
      "layers": {
        "clause": {"labels": ["a", "b"], "min_support": 2},
        "annex":  {"labels": ["x"],      "min_support": 2}
      },
      "relation_types": {
        "ref": {"from_layer": "clause", "to_layer": "annex", "min_support": 2}
      }
    }

最少支持团队数至少为 2（二方）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Mapping

MIN_SUPPORT_LOWER_BOUND = 2


@dataclass(frozen=True)
class LayerPolicy:
    name: str
    labels: tuple
    min_support: int


@dataclass(frozen=True)
class RelationPolicy:
    name: str
    from_layer: str
    to_layer: str
    min_support: int


@dataclass(frozen=True)
class Policy:
    layers: Mapping[str, LayerPolicy]
    relation_types: Mapping[str, RelationPolicy]

    def layer(self, name: str) -> LayerPolicy:
        return self.layers[name]

    def relation(self, name: str) -> RelationPolicy:
        return self.relation_types[name]


def load_policy(path: str) -> Policy:
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return parse_policy(raw)


def parse_policy(raw: dict) -> Policy:
    if not isinstance(raw, dict):
        raise ValueError("policy must be a JSON object")
    layers_raw = raw.get("layers")
    rels_raw = raw.get("relation_types", {})
    if not isinstance(layers_raw, dict) or not layers_raw:
        raise ValueError("policy.layers must be a non-empty object")

    layers: dict[str, LayerPolicy] = {}
    for name, spec in layers_raw.items():
        if not isinstance(spec, dict):
            raise ValueError(f"policy.layers[{name}] must be an object")
        labels = spec.get("labels")
        if not isinstance(labels, list) or not labels or any(
            not isinstance(x, str) or not x for x in labels
        ):
            raise ValueError(f"policy.layers[{name}].labels must be non-empty strings")
        min_support = spec.get("min_support")
        if not isinstance(min_support, int) or isinstance(min_support, bool):
            raise ValueError(f"policy.layers[{name}].min_support must be an integer")
        if min_support < MIN_SUPPORT_LOWER_BOUND:
            raise ValueError(
                f"policy.layers[{name}].min_support must be >="
                f" {MIN_SUPPORT_LOWER_BOUND}"
            )
        layers[name] = LayerPolicy(name, tuple(labels), min_support)

    rels: dict[str, RelationPolicy] = {}
    if not isinstance(rels_raw, dict):
        raise ValueError("policy.relation_types must be an object")
    for name, spec in rels_raw.items():
        if not isinstance(spec, dict):
            raise ValueError(f"policy.relation_types[{name}] must be an object")
        from_layer = spec.get("from_layer")
        to_layer = spec.get("to_layer")
        min_support = spec.get("min_support")
        if from_layer not in layers or to_layer not in layers:
            raise ValueError(
                f"policy.relation_types[{name}] endpoint layers must be declared"
            )
        if not isinstance(min_support, int) or isinstance(min_support, bool):
            raise ValueError(
                f"policy.relation_types[{name}].min_support must be an integer"
            )
        if min_support < MIN_SUPPORT_LOWER_BOUND:
            raise ValueError(
                f"policy.relation_types[{name}].min_support must be >="
                f" {MIN_SUPPORT_LOWER_BOUND}"
            )
        rels[name] = RelationPolicy(name, from_layer, to_layer, min_support)

    return Policy(layers=layers, relation_types=rels)
