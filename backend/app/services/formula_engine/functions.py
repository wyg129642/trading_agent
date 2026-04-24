"""Excel-compatible function library for the formula engine.

Implements ~80 of the most commonly used Excel functions. Each function
takes already-evaluated arguments (Python values) and returns a Python
value. Ranges are passed as flat lists.

Unknown / special values follow Excel semantics as closely as practical:

* ``None`` means empty cell; treated as 0 in arithmetic, "" in concat.
* Strings that look like numbers are coerced in arithmetic contexts.
* Division by zero raises ``ZeroDivisionError`` — the engine wraps this
  into ``#DIV/0!``.

We keep it pure-Python — no numpy dependency.
"""
from __future__ import annotations

import math
import statistics
from datetime import date, datetime, timedelta
from typing import Any, Callable

EXCEL_FUNCTIONS: dict[str, Callable[..., Any]] = {}


def register_function(name: str, fn: Callable[..., Any]) -> None:
    """Register (or override) an Excel-compatible function by UPPERCASE name."""
    EXCEL_FUNCTIONS[name.upper()] = fn


def _flatten(*args) -> list:
    """Flatten nested lists into a single list."""
    out: list = []
    for a in args:
        if isinstance(a, (list, tuple)):
            out.extend(_flatten(*a))
        else:
            out.append(a)
    return out


def _numeric(vals: list) -> list[float]:
    """Keep only numeric values (skips None, str, bool except as numeric)."""
    out: list[float] = []
    for v in vals:
        if v is None:
            continue
        if isinstance(v, bool):
            out.append(1.0 if v else 0.0)
        elif isinstance(v, (int, float)):
            if not (isinstance(v, float) and math.isnan(v)):
                out.append(float(v))
        elif isinstance(v, str):
            try:
                out.append(float(v))
            except ValueError:
                continue
    return out


def _to_number(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().rstrip("%")
        try:
            n = float(s)
            return n / 100.0 if v.rstrip().endswith("%") else n
        except ValueError:
            raise ValueError(f"#VALUE! — cannot convert {v!r} to number")
    raise ValueError(f"#VALUE! — cannot convert {type(v).__name__} to number")


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.lower() not in ("", "false", "0")
    return bool(v)


# ── Arithmetic / Stats ──────────────────────────────────────────

register_function("SUM", lambda *a: sum(_numeric(_flatten(*a))))
register_function("AVERAGE", lambda *a: statistics.fmean(_numeric(_flatten(*a))) if _numeric(_flatten(*a)) else 0)
register_function("MEDIAN", lambda *a: statistics.median(_numeric(_flatten(*a))) if _numeric(_flatten(*a)) else 0)
register_function("COUNT", lambda *a: len(_numeric(_flatten(*a))))
register_function("COUNTA", lambda *a: sum(1 for v in _flatten(*a) if v is not None and v != ""))
register_function("MIN", lambda *a: min(_numeric(_flatten(*a))) if _numeric(_flatten(*a)) else 0)
register_function("MAX", lambda *a: max(_numeric(_flatten(*a))) if _numeric(_flatten(*a)) else 0)
register_function("PRODUCT", lambda *a: math.prod(_numeric(_flatten(*a))) if _numeric(_flatten(*a)) else 0)


def _stdev(*a, pop: bool):
    vs = _numeric(_flatten(*a))
    if len(vs) < (1 if pop else 2):
        return 0.0
    return statistics.pstdev(vs) if pop else statistics.stdev(vs)


register_function("STDEV", lambda *a: _stdev(*a, pop=False))
register_function("STDEV.S", lambda *a: _stdev(*a, pop=False))
register_function("STDEV.P", lambda *a: _stdev(*a, pop=True))
register_function("STDEVP", lambda *a: _stdev(*a, pop=True))


def _var(*a, pop: bool):
    vs = _numeric(_flatten(*a))
    if len(vs) < (1 if pop else 2):
        return 0.0
    return statistics.pvariance(vs) if pop else statistics.variance(vs)


register_function("VAR", lambda *a: _var(*a, pop=False))
register_function("VAR.S", lambda *a: _var(*a, pop=False))
register_function("VAR.P", lambda *a: _var(*a, pop=True))
register_function("VARP", lambda *a: _var(*a, pop=True))


# ── Math ────────────────────────────────────────────────────────

register_function("ABS", lambda v: abs(_to_number(v)))


def _excel_round(v, d=0):
    """Excel's ROUND: half-away-from-zero (not banker's)."""
    n = _to_number(v)
    d = int(_to_number(d))
    mult = 10 ** d
    scaled = n * mult
    if scaled >= 0:
        rounded = math.floor(scaled + 0.5)
    else:
        rounded = -math.floor(-scaled + 0.5)
    return rounded / mult


register_function("ROUND", _excel_round)


def _rounddir(v, d, up: bool):
    n = _to_number(v)
    d = int(_to_number(d))
    mult = 10 ** d
    if up:
        return math.ceil(abs(n) * mult) / mult * (1 if n >= 0 else -1)
    return math.floor(abs(n) * mult) / mult * (1 if n >= 0 else -1)


register_function("ROUNDUP", lambda v, d=0: _rounddir(v, d, up=True))
register_function("ROUNDDOWN", lambda v, d=0: _rounddir(v, d, up=False))
register_function("CEILING", lambda v, sig=1: math.ceil(_to_number(v) / _to_number(sig)) * _to_number(sig))
register_function("FLOOR", lambda v, sig=1: math.floor(_to_number(v) / _to_number(sig)) * _to_number(sig))
register_function("INT", lambda v: math.floor(_to_number(v)))
register_function("TRUNC", lambda v, d=0: (lambda n, m=10 ** int(_to_number(d)): math.trunc(n * m) / m)(_to_number(v)))
register_function("MOD", lambda a, b: _to_number(a) % _to_number(b))
register_function("POWER", lambda a, b: _to_number(a) ** _to_number(b))
register_function("SQRT", lambda v: math.sqrt(_to_number(v)))
register_function("EXP", lambda v: math.exp(_to_number(v)))
register_function("LN", lambda v: math.log(_to_number(v)))
register_function("LOG", lambda v, b=10: math.log(_to_number(v), _to_number(b)))
register_function("LOG10", lambda v: math.log10(_to_number(v)))
register_function("SIGN", lambda v: (0 if _to_number(v) == 0 else (1 if _to_number(v) > 0 else -1)))
register_function("PI", lambda: math.pi)


# ── Logical ─────────────────────────────────────────────────────

def _if(cond, tv, fv=False):
    return tv if _truthy(cond) else fv


register_function("IF", _if)


def _ifs(*args):
    if len(args) % 2 != 0:
        raise ValueError("#N/A — IFS requires pairs of (condition, value)")
    for cond, val in zip(args[::2], args[1::2]):
        if _truthy(cond):
            return val
    raise ValueError("#N/A — no IFS condition matched")


register_function("IFS", _ifs)


def _iferror(v, fallback):
    # Caller-level error handling: an exception during eval of `v` would prevent
    # this function from running. Thus we intentionally rely on the engine to
    # intercept errors — see evaluator._eval_call's IFERROR special case.
    return v


register_function("IFERROR", _iferror)
register_function("IFNA", _iferror)

register_function("AND", lambda *a: all(_truthy(x) for x in _flatten(*a)))
register_function("OR", lambda *a: any(_truthy(x) for x in _flatten(*a)))
register_function("NOT", lambda v: not _truthy(v))
register_function("XOR", lambda *a: sum(1 for x in _flatten(*a) if _truthy(x)) % 2 == 1)
register_function("TRUE", lambda: True)
register_function("FALSE", lambda: False)


def _switch(expr, *rest):
    if len(rest) < 2:
        raise ValueError("#N/A — SWITCH requires at least one (match,value) pair")
    default = None
    pairs: list
    if len(rest) % 2 == 1:
        pairs = list(rest[:-1])
        default = rest[-1]
    else:
        pairs = list(rest)
    for match, val in zip(pairs[::2], pairs[1::2]):
        if expr == match:
            return val
    if default is not None:
        return default
    raise ValueError("#N/A — no SWITCH match")


register_function("SWITCH", _switch)


# ── Conditional aggregation ─────────────────────────────────────

def _match_criteria(value: Any, criteria: Any) -> bool:
    """Excel criteria semantics: '>10', '<=5', '<>foo', or equality."""
    if isinstance(criteria, str):
        c = criteria.strip()
        for op, fn in [
            (">=", lambda a, b: _to_number(a) >= _to_number(b)),
            ("<=", lambda a, b: _to_number(a) <= _to_number(b)),
            ("<>", lambda a, b: a != b),
            ("!=", lambda a, b: a != b),
            (">", lambda a, b: _to_number(a) > _to_number(b)),
            ("<", lambda a, b: _to_number(a) < _to_number(b)),
            ("=", lambda a, b: a == b),
        ]:
            if c.startswith(op):
                rhs = c[len(op):]
                try:
                    rhs_n = float(rhs)
                    return fn(value, rhs_n)
                except ValueError:
                    return fn(value, rhs)
        return value == c
    return value == criteria


def _countif(rng, crit):
    flat = _flatten(rng)
    return sum(1 for v in flat if _match_criteria(v, crit))


def _sumif(rng, crit, sum_rng=None):
    flat = _flatten(rng)
    srng = _flatten(sum_rng) if sum_rng is not None else flat
    # If sum_rng shorter, treat as truncated (Excel pads)
    total = 0.0
    for i, v in enumerate(flat):
        if _match_criteria(v, crit):
            if i < len(srng):
                try:
                    total += _to_number(srng[i])
                except ValueError:
                    pass
    return total


def _averageif(rng, crit, sum_rng=None):
    flat = _flatten(rng)
    srng = _flatten(sum_rng) if sum_rng is not None else flat
    vs: list[float] = []
    for i, v in enumerate(flat):
        if _match_criteria(v, crit) and i < len(srng):
            try:
                vs.append(_to_number(srng[i]))
            except ValueError:
                pass
    return sum(vs) / len(vs) if vs else 0.0


def _multi_criteria_iter(ranges: list, criteria: list):
    n = min(len(r) for r in ranges) if ranges else 0
    for i in range(n):
        if all(_match_criteria(r[i], c) for r, c in zip(ranges, criteria)):
            yield i


def _sumifs(sum_rng, *args):
    if len(args) % 2 != 0:
        raise ValueError("#N/A — SUMIFS requires (range, criteria) pairs")
    srng = _flatten(sum_rng)
    ranges = [_flatten(r) for r in args[::2]]
    criteria = list(args[1::2])
    total = 0.0
    for i in _multi_criteria_iter(ranges, criteria):
        if i < len(srng):
            try:
                total += _to_number(srng[i])
            except ValueError:
                pass
    return total


def _countifs(*args):
    if len(args) % 2 != 0 or len(args) == 0:
        raise ValueError("#N/A — COUNTIFS requires (range, criteria) pairs")
    ranges = [_flatten(r) for r in args[::2]]
    criteria = list(args[1::2])
    return sum(1 for _ in _multi_criteria_iter(ranges, criteria))


def _averageifs(avg_rng, *args):
    if len(args) % 2 != 0:
        raise ValueError("#N/A — AVERAGEIFS requires (range, criteria) pairs")
    arng = _flatten(avg_rng)
    ranges = [_flatten(r) for r in args[::2]]
    criteria = list(args[1::2])
    vs = []
    for i in _multi_criteria_iter(ranges, criteria):
        if i < len(arng):
            try:
                vs.append(_to_number(arng[i]))
            except ValueError:
                pass
    return sum(vs) / len(vs) if vs else 0.0


register_function("COUNTIF", _countif)
register_function("SUMIF", _sumif)
register_function("AVERAGEIF", _averageif)
register_function("SUMIFS", _sumifs)
register_function("COUNTIFS", _countifs)
register_function("AVERAGEIFS", _averageifs)


def _maxifs(max_rng, *args):
    if len(args) % 2 != 0:
        raise ValueError("#N/A — MAXIFS requires (range, criteria) pairs")
    mrng = _flatten(max_rng)
    ranges = [_flatten(r) for r in args[::2]]
    criteria = list(args[1::2])
    best: float | None = None
    for i in _multi_criteria_iter(ranges, criteria):
        if i < len(mrng):
            try:
                n = _to_number(mrng[i])
                if best is None or n > best:
                    best = n
            except ValueError:
                pass
    return best if best is not None else 0.0


def _minifs(min_rng, *args):
    if len(args) % 2 != 0:
        raise ValueError("#N/A — MINIFS requires (range, criteria) pairs")
    mrng = _flatten(min_rng)
    ranges = [_flatten(r) for r in args[::2]]
    criteria = list(args[1::2])
    best: float | None = None
    for i in _multi_criteria_iter(ranges, criteria):
        if i < len(mrng):
            try:
                n = _to_number(mrng[i])
                if best is None or n < best:
                    best = n
            except ValueError:
                pass
    return best if best is not None else 0.0


register_function("MAXIFS", _maxifs)
register_function("MINIFS", _minifs)


# ── Lookup ──────────────────────────────────────────────────────

def _vlookup(lookup, table, col_index, approx=False):
    if not isinstance(table, list) or not table or not isinstance(table[0], list):
        raise ValueError("#N/A — VLOOKUP table must be 2-D list")
    col_index = int(_to_number(col_index))
    for row in table:
        if not row:
            continue
        if (approx and isinstance(row[0], (int, float)) and row[0] <= _to_number(lookup)) or row[0] == lookup:
            if col_index - 1 < len(row):
                return row[col_index - 1]
    raise ValueError("#N/A — VLOOKUP no match")


def _hlookup(lookup, table, row_index, approx=False):
    if not isinstance(table, list) or not table or not isinstance(table[0], list):
        raise ValueError("#N/A — HLOOKUP table must be 2-D list")
    row_index = int(_to_number(row_index))
    header = table[0]
    for j, v in enumerate(header):
        if (approx and isinstance(v, (int, float)) and v <= _to_number(lookup)) or v == lookup:
            if row_index - 1 < len(table) and j < len(table[row_index - 1]):
                return table[row_index - 1][j]
    raise ValueError("#N/A — HLOOKUP no match")


def _index(table, row, col=None):
    row = int(_to_number(row))
    if col is not None:
        col = int(_to_number(col))
    if isinstance(table, list) and table and isinstance(table[0], list):
        r = table[row - 1]
        return r[col - 1] if col else r
    if isinstance(table, list):
        return table[row - 1]
    return table


def _match(lookup, array, match_type=1):
    arr = _flatten(array)
    mt = int(_to_number(match_type)) if match_type is not None else 1
    if mt == 0:
        for i, v in enumerate(arr):
            if v == lookup:
                return i + 1
        raise ValueError("#N/A — MATCH no match")
    if mt == 1:
        best = None
        for i, v in enumerate(arr):
            try:
                if _to_number(v) <= _to_number(lookup):
                    best = i + 1
            except ValueError:
                pass
        if best is None:
            raise ValueError("#N/A — MATCH no match")
        return best
    # mt == -1 (descending)
    for i, v in enumerate(arr):
        try:
            if _to_number(v) >= _to_number(lookup):
                return i + 1
        except ValueError:
            pass
    raise ValueError("#N/A — MATCH no match")


def _choose(idx, *vals):
    idx = int(_to_number(idx))
    if 1 <= idx <= len(vals):
        return vals[idx - 1]
    raise ValueError("#VALUE! — CHOOSE index out of range")


register_function("VLOOKUP", _vlookup)
register_function("HLOOKUP", _hlookup)
register_function("INDEX", _index)
register_function("MATCH", _match)
register_function("CHOOSE", _choose)


# ── Text ────────────────────────────────────────────────────────

def _concat(*args):
    return "".join("" if a is None else str(a) for a in _flatten(*args))


register_function("CONCAT", _concat)
register_function("CONCATENATE", _concat)
register_function("LEFT", lambda s, n=1: str(s)[:int(_to_number(n))])
register_function("RIGHT", lambda s, n=1: str(s)[-int(_to_number(n)):] if int(_to_number(n)) > 0 else "")
register_function("MID", lambda s, start, n: str(s)[max(int(_to_number(start)) - 1, 0):max(int(_to_number(start)) - 1, 0) + int(_to_number(n))])
register_function("LEN", lambda s: len(str(s)) if s is not None else 0)
register_function("UPPER", lambda s: str(s).upper())
register_function("LOWER", lambda s: str(s).lower())
register_function("TRIM", lambda s: " ".join(str(s).split()))
register_function("FIND", lambda needle, s, start=1: str(s).find(str(needle), int(_to_number(start)) - 1) + 1 if str(needle) in str(s)[int(_to_number(start)) - 1:] else (_ for _ in ()).throw(ValueError("#VALUE!")))
register_function("SEARCH", lambda needle, s, start=1: (str(s).lower().find(str(needle).lower(), int(_to_number(start)) - 1) + 1) if str(needle).lower() in str(s).lower()[int(_to_number(start)) - 1:] else (_ for _ in ()).throw(ValueError("#VALUE!")))
register_function("SUBSTITUTE", lambda s, old, new, n=None: (str(s).replace(str(old), str(new)) if n is None else _substitute_nth(str(s), str(old), str(new), int(_to_number(n)))))
register_function("REPLACE", lambda s, start, n, new: str(s)[:int(_to_number(start)) - 1] + str(new) + str(s)[int(_to_number(start)) - 1 + int(_to_number(n)):])


def _substitute_nth(s: str, old: str, new: str, n: int) -> str:
    parts = s.split(old)
    if n < 1 or n >= len(parts):
        return s
    return old.join(parts[:n]) + new + old.join(parts[n:])


def _text_format(v, fmt):
    """TEXT(number, format) — a small subset of Excel format codes.

    Supported:
      0.00, #,##0.00, 0%, 0.00%
      plus pass-through for arbitrary strings that contain one of the above tokens.
    """
    try:
        n = _to_number(v)
    except ValueError:
        return str(v)
    fmt = str(fmt)
    pct = "%" in fmt
    if pct:
        n = n * 100
    # Count decimals
    if "." in fmt:
        dec = len(fmt.rsplit(".", 1)[1].split("%", 1)[0].rstrip("0#"))
        dec = len(fmt.rsplit(".", 1)[1].split("%", 1)[0])
    else:
        dec = 0
    thousands = "," in fmt
    if thousands:
        out = f"{n:,.{dec}f}"
    else:
        out = f"{n:.{dec}f}"
    if pct:
        out += "%"
    return out


register_function("TEXT", _text_format)
register_function("VALUE", lambda v: _to_number(v))


# ── Date ────────────────────────────────────────────────────────

def _coerce_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, (int, float)):
        # Excel serial date (1 = 1900-01-01 roughly; we use a 1970 epoch for simplicity)
        return date(1970, 1, 1) + timedelta(days=int(v))
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(v, fmt).date()
            except ValueError:
                pass
    raise ValueError(f"#VALUE! — cannot coerce to date: {v!r}")


register_function("DATE", lambda y, m, d: date(int(_to_number(y)), int(_to_number(m)), int(_to_number(d))))
register_function("TODAY", lambda: date.today())
register_function("NOW", lambda: datetime.now())
register_function("YEAR", lambda v: _coerce_date(v).year)
register_function("MONTH", lambda v: _coerce_date(v).month)
register_function("DAY", lambda v: _coerce_date(v).day)
register_function("DATEDIF", lambda a, b, unit: _datedif(a, b, unit))


def _datedif(a, b, unit):
    d1, d2 = _coerce_date(a), _coerce_date(b)
    unit = str(unit).upper()
    if unit == "D":
        return (d2 - d1).days
    if unit == "M":
        return (d2.year - d1.year) * 12 + (d2.month - d1.month) - (1 if d2.day < d1.day else 0)
    if unit == "Y":
        yrs = d2.year - d1.year
        if (d2.month, d2.day) < (d1.month, d1.day):
            yrs -= 1
        return yrs
    raise ValueError("#NUM! — DATEDIF unit must be D/M/Y")


# ── Financial ───────────────────────────────────────────────────

def _pv(rate, nper, pmt, fv=0, when=0):
    r = _to_number(rate)
    n = int(_to_number(nper))
    pmt = _to_number(pmt)
    fv = _to_number(fv)
    when = _to_number(when)
    if r == 0:
        return -(pmt * n + fv)
    return -(pmt * (1 + r * when) * ((1 + r) ** n - 1) / r + fv) / (1 + r) ** n


def _fv(rate, nper, pmt, pv=0, when=0):
    r = _to_number(rate)
    n = int(_to_number(nper))
    pmt = _to_number(pmt)
    pv = _to_number(pv)
    when = _to_number(when)
    if r == 0:
        return -(pv + pmt * n)
    return -(pv * (1 + r) ** n + pmt * (1 + r * when) * ((1 + r) ** n - 1) / r)


def _npv(rate, *cashflows):
    r = _to_number(rate)
    flat = _flatten(*cashflows)
    return sum(_to_number(v) / (1 + r) ** (i + 1) for i, v in enumerate(flat))


def _irr(cashflows, guess=0.1):
    flat = _numeric(_flatten(cashflows))
    if len(flat) < 2:
        raise ValueError("#NUM! — IRR needs at least 2 cashflows")
    # Newton–Raphson
    r = _to_number(guess)
    for _ in range(200):
        f = sum(v / (1 + r) ** i for i, v in enumerate(flat))
        df = sum(-i * v / (1 + r) ** (i + 1) for i, v in enumerate(flat))
        if df == 0:
            break
        r_new = r - f / df
        if abs(r_new - r) < 1e-10:
            return r_new
        r = r_new
    return r


register_function("PV", _pv)
register_function("FV", _fv)
register_function("NPV", _npv)
register_function("IRR", _irr)


# ── Finance/industry helpers (analyst-friendly extras) ─────────

def _cagr(begin, end, years):
    begin = _to_number(begin)
    end = _to_number(end)
    years = _to_number(years)
    if begin == 0 or years <= 0:
        raise ValueError("#NUM! — CAGR needs begin>0 and years>0")
    return (end / begin) ** (1 / years) - 1


def _yoy(curr, prev):
    curr = _to_number(curr)
    prev = _to_number(prev)
    if prev == 0:
        raise ValueError("#DIV/0! — YoY previous is zero")
    return curr / prev - 1


register_function("CAGR", _cagr)
register_function("YOY", _yoy)


# ── Informational ───────────────────────────────────────────────

register_function("ISNUMBER", lambda v: isinstance(v, (int, float)) and not isinstance(v, bool))
register_function("ISTEXT", lambda v: isinstance(v, str))
register_function("ISBLANK", lambda v: v is None or v == "")
register_function("ISERROR", lambda v: False)  # engine intercepts; by the time we see v it's valid
register_function("ISNA", lambda v: False)
register_function("NA", lambda: (_ for _ in ()).throw(ValueError("#N/A")))
