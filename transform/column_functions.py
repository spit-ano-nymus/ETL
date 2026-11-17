"""
column_functions.py
-------------------
Python functions that one column of a final (analyst) table can be run
through — for work SQL cannot do, above all calling an outside service.

This is where YOU add your own. A function gets the column's distinct values
and returns the new value for each:

    @register_column_fn("my_fn")
    def my_fn(values: list[str]) -> dict[str, str]:
        return {v: do_something(v) for v in values}

Then name it in the mapper, for the column that needs it:

    UPDATE meta.etl_column_map
       SET py_func = 'my_fn'
     WHERE table_key = 'customer' AND source_col = 'notes';

How it runs:
  - after the final table is built (public -> analyst), in the same
    transaction, so a failure here rolls the whole run back
  - only distinct, non-NULL values are passed
  - every result is cached in meta.py_func_cache, so the function is called
    only for values the pipeline has not seen before. Clear the cache for one
    function to force a recompute:
        DELETE FROM meta.py_func_cache WHERE func_name = 'my_fn';
  - the column must be target_type 'text'

A value the function leaves out, or maps to None, keeps the value it had.
"""

from __future__ import annotations

import logging
import os
import unicodedata

from core.registry import register_column_fn

logger = logging.getLogger(__name__)


# ── pure-Python examples ──────────────────────────────────────────────────────

@register_column_fn("strip_nikud")
def strip_nikud(values: list[str]) -> dict[str, str]:
    """
    Remove Hebrew vowel points and cantillation marks, keeping the letters.
    'בְּרֵאשִׁית' -> 'בראשית'. Awkward in SQL, trivial here.
    """
    out: dict[str, str] = {}
    for v in values:
        out[v] = "".join(c for c in unicodedata.normalize("NFD", v)
                         if not unicodedata.combining(c))
    return out


@register_column_fn("normalize_space_py")
def normalize_space_py(values: list[str]) -> dict[str, str]:
    """Trim and collapse any whitespace, including the odd Unicode kinds."""
    return {v: " ".join(v.split()) for v in values}


# ── translation through an outside engine ─────────────────────────────────────
#
# Wire your own engine into _call_translation_api below. Everything around it
# — distinct values, caching, writing back — is already handled.

def _call_translation_api(texts: list[str]) -> list[str]:
    """
    Translate `texts` and return the translations in the same order.

    Implement this against your own engine. A sketch of the usual shape:

        import requests
        url = os.environ["TRANSLATE_URL"]
        key = os.environ["TRANSLATE_KEY"]
        out: list[str] = []
        for i in range(0, len(texts), 50):              # batch, do not send one by one
            batch = texts[i:i + 50]
            r = requests.post(url, json={"q": batch, "target": "he"},
                              headers={"Authorization": f"Bearer {key}"}, timeout=60)
            r.raise_for_status()
            out.extend(item["translatedText"] for item in r.json()["translations"])
        return out

    Keep two things in mind:
      - batch the requests; one call per row is slow and expensive
      - let a real failure raise. The run then fails and rolls back, which is
        better than a half-translated table. Values already cached from
        earlier runs are not re-sent.
    """
    raise NotImplementedError(
        "Translation is not wired up yet: implement _call_translation_api in "
        "transform/column_functions.py (it receives a list of texts and returns "
        "their translations in the same order)."
    )


@register_column_fn("translate_he")
def translate_he(values: list[str]) -> dict[str, str]:
    """
    Translate a text column through the engine configured above.

    Only values not already in meta.py_func_cache reach this function, so the
    engine is paid for once per distinct text.
    """
    if not values:
        return {}
    logger.info("translate_he: %d new value(s) to translate", len(values))
    translated = _call_translation_api(values)
    if len(translated) != len(values):
        raise ValueError(
            f"translate_he: the engine returned {len(translated)} translations "
            f"for {len(values)} texts — they must match, in the same order"
        )
    return dict(zip(values, translated))
