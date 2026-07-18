"""Generic `{variable}` template substitution for manifest input fields.

This is deliberately not LLM-specific — it's plain string interpolation against
whatever is currently in the shared context (sibling inputs on the same step,
or an earlier pipeline step's output), so it's useful for any module, not just
one that happens to call a real language model.
"""
import re
from typing import Callable, Optional

_PATTERN = re.compile(r"\{(\w+(?:\.\w+)*)\}")


def resolve_path(path: str, lookup: Callable[[str], Optional[object]]) -> Optional[object]:
    """Resolve a possibly dotted `path` (e.g. `insight.risk_level`) against
    `lookup` for the base name, then walk each remaining segment as a dict
    key. Returns None the moment a segment is missing or the value isn't a
    dict — a bad or too-deep path just resolves to "not found," same as an
    unknown flat name."""
    parts = path.split(".")
    value = lookup(parts[0])
    for part in parts[1:]:
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    return value


def render_template(template: str, lookup: Callable[[str], Optional[object]]) -> str:
    """Replace every `{name}` or `{name.nested.path}` in `template` with
    `str(resolve_path(...))`.

    A path `lookup` can't resolve is left untouched (`{typo}` stays literal)
    rather than silently vanishing, so a bad reference is obvious in the output.
    """

    def _replace(match: "re.Match[str]") -> str:
        value = resolve_path(match.group(1), lookup)
        return str(value) if value is not None else match.group(0)

    return _PATTERN.sub(_replace, template)


def interpolate_template_fields(manifest: dict, values: dict, lookup: Callable[[str], Optional[object]]) -> dict:
    """Apply render_template to every string value of this manifest's `template`-type
    input fields. Called before a module ever sees its inputs, so `BaseModule`
    implementations stay completely unaware this mechanism exists — they just read
    an already-substituted string via `context.get(field_name)`."""
    result = dict(values)
    for field in manifest.get("inputs", []):
        if field.get("type") == "template":
            value = result.get(field["name"])
            if isinstance(value, str):
                result[field["name"]] = render_template(value, lookup)
    return result
