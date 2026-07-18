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

    nodes = []
    for index, step in enumerate(steps):
        if step.get("type") == "parallel":
            branches = step.get("branches") or []
            label = step.get("name") or "parallel"
            branch_names = ", ".join(b["name"] for b in branches)
            nodes.append(
                {
                    "index": index,
                    "tier": "parallel",
                    "name": f"{label} ⫲ [{branch_names}]",
                    "branches": [{"tier": b["tier"], "name": b["name"]} for b in branches],
                }
            )
        else:
            nodes.append({"index": index, "tier": step["tier"], "name": step["name"]})

    edges = []
    for index, step in enumerate(steps):
        if index > 0:
            edges.append({"from": index - 1, "to": index, "kind": "sequence"})
        # A group's slot aggregates its branches' mapping edges — each branch
        # may pull from any earlier top-level step, same as a module step.
        branch_steps = step.get("branches") or [step]
        for sub in branch_steps:
            for field, mapping in (sub.get("mappings") or {}).items():
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
