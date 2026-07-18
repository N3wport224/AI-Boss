"""Builds a DAG view of a saved pipeline: which step feeds which, and via
which field. Pipeline steps already execute strictly in list order, and a
mapping can only reference a strictly earlier step (enforced by
`pipelines.validate_pipeline`) — so the step list itself is already a valid
topological order, and the mapping edges alone form a genuine DAG rather than
a simple chain, since a step can pull from any earlier step, not just the one
immediately before it.
"""


def build_pipeline_graph(definition: dict) -> dict:
    steps = definition.get("steps") or []
    nodes = [
        {"index": index, "tier": step["tier"], "name": step["name"]}
        for index, step in enumerate(steps)
    ]

    edges = []
    for index, step in enumerate(steps):
        if index > 0:
            edges.append({"from": index - 1, "to": index, "kind": "sequence"})
        for field, mapping in (step.get("mappings") or {}).items():
            edges.append(
                {
                    "from": mapping["step"],
                    "to": index,
                    "kind": "mapping",
                    "field": field,
                    "output": mapping["output"],
                }
            )

    return {"nodes": nodes, "edges": edges}
