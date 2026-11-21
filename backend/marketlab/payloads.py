"""Fast strict transport encoding, deliberately separate from canonical hashing.

Dataset, event and model identities MUST keep contracts.canonical/identity.
Transport field order is not an identity and is decoded before comparison.
"""

import math
import orjson


def wire(value):
    stack = [value]
    seen = set()
    while stack:
        item = stack.pop()
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("Non-finite transport value")
        elif isinstance(item, (dict, list, tuple)):
            key = id(item)
            if key in seen:
                continue
            seen.add(key)
            stack.extend(item.values() if isinstance(item, dict) else item)
    return orjson.dumps(value, default=str).decode("utf-8")
