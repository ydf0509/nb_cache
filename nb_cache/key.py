# -*- coding: utf-8 -*-
"""Cache key generation and template utilities."""
import hashlib
import inspect
import re

# Matches {param}, {param:fmt}, {param.attr}, {param.attr:fmt}
_TEMPLATE_PARAM_RE = re.compile(r'\{([\w.]+)(?::(\w+))?\}')


def get_func_name(func):
    """Get a stable, qualified name for a function."""
    module = getattr(func, '__module__', '') or ''
    qualname = getattr(func, '__qualname__', '') or getattr(func, '__name__', '')
    return "{}:{}".format(module, qualname)


def get_cache_key(func, key_template, args, kwargs):
    """Build a concrete cache key from a function call and optional template.

    key_template can be:
      - None: auto-generate from function name + bound arguments
      - str: render as a template, supporting {param}, {param.attr}, {param:hash/lower}
      - callable: call with the same (args, kwargs) and return the key string
    """
    if key_template is None:
        key = _auto_key(func, args, kwargs)
    elif callable(key_template):
        bound = _bind_arguments(func, args, kwargs)
        positional = list(bound.values())
        key = str(key_template(*positional))
    else:
        key = _render_template(func, key_template, args, kwargs)

    return key


def get_cache_key_template(func, key=None, prefix="", key_include_func=True):
    """Build a key template (string or callable) for a function.

    When key is a callable, it is stored as-is and will be called at cache time.
    When key is a string template, it is prefixed with func_name (if include_func_name=True).
    When key is None, auto-generate a template from the function signature.

    Args:
        func: The decorated function.
        key: Key template string or callable. None means auto-generate.
        prefix: Key prefix string (from decorator or setup).
        key_include_func: If False, the module path and function name are NOT
            included in the generated key. Useful when you want a short,
            purely business-logic key (e.g. ``aiof:3_4`` instead of
            ``__main__:aio_fun:aiof:3_4``).
    """
    func_name = get_func_name(func)
    if key is not None:
        if callable(key):
            return key
        if key_include_func:
            if prefix:
                return "{}:{}:{}".format(prefix, func_name, key)
            return "{}:{}".format(func_name, key)
        else:
            if prefix:
                return "{}:{}".format(prefix, key)
            return key

    sig = inspect.signature(func)
    parts = []
    for name, param in sig.parameters.items():
        if name in ('self', 'cls'):
            continue
        parts.append("{{{name}}}".format(name=name))

    template = ":".join(parts) if parts else ""
    if key_include_func:
        if prefix:
            return "{}:{}:{}".format(prefix, func_name, template)
        return "{}:{}".format(func_name, template)
    else:
        if prefix:
            return "{}:{}".format(prefix, template) if template else prefix
        return template


def _resolve_attr(val, attr_path):
    """Resolve a dotted attribute path on a value.

    Supports both object attributes and dict key access.
    e.g. attr_path="id" on {"id": 1} returns 1
         attr_path="address.city" on obj.address.city returns city value
    """
    for part in attr_path.split('.'):
        if isinstance(val, dict):
            val = val.get(part, '')
        else:
            val = getattr(val, part, '')
    return val


def _render_template(func, template, args, kwargs):
    """Render a key template string with actual argument values.

    Supports:
      {param}           — plain value
      {param.attr}      — attribute/dict-key access
      {param:hash}      — md5 hash of value
      {param:lower}     — lowercase string
      {param.attr:hash} — hash of nested attribute
    """
    bound = _bind_arguments(func, args, kwargs)

    def _replacer(m):
        expr = m.group(1)   # e.g. "user", "user.id", "filters"
        fmt = m.group(2)    # e.g. "hash", "lower", or None

        parts = expr.split('.', 1)
        param_name = parts[0]
        attr_path = parts[1] if len(parts) > 1 else None

        val = bound.get(param_name, '')
        if attr_path:
            val = _resolve_attr(val, attr_path)

        if fmt == 'hash':
            return _hash_value(val)
        if fmt == 'lower':
            return str(val).lower()
        return str(val)

    return _TEMPLATE_PARAM_RE.sub(_replacer, template)


def _auto_key(func, args, kwargs):
    """Auto-generate a cache key from function name + arguments."""
    func_name = get_func_name(func)
    bound = _bind_arguments(func, args, kwargs)
    parts = [func_name]
    for name, val in sorted(bound.items()):
        if name == 'self' or name == 'cls':
            continue
        parts.append("{}={}".format(name, _safe_str(val)))
    return ":".join(parts)


def _bind_arguments(func, args, kwargs):
    """Bind positional and keyword arguments to parameter names."""
    try:
        sig = inspect.signature(func)
        ba = sig.bind(*args, **kwargs)
        ba.apply_defaults()
        return dict(ba.arguments)
    except (ValueError, TypeError):
        result = {}
        for i, v in enumerate(args):
            result["arg{}".format(i)] = v
        result.update(kwargs)
        return result


def _hash_value(val):
    return hashlib.md5(str(val).encode()).hexdigest()[:8]


def _safe_str(val):
    """Convert a value to a string safe for use in a cache key."""
    if isinstance(val, (list, tuple, set, frozenset)):
        return _hash_value(val)
    if isinstance(val, dict):
        return _hash_value(sorted(val.items()))
    return str(val)
