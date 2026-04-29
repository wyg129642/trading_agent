"""crawl/local_ai_summary — qwen-plus card-preview summarizer for portfolio docs.

For each portfolio holding, scan all relevant Mongo collections (alphapai /
jinmen / gangtise / funda / alphaengine / acecamp / meritco / ir_filings)
and write a short card-preview summary into ``local_ai_summary.tldr`` if no
native summary already exists.

Field shape::

    local_ai_summary: {
      tldr:          str,    # 80-160 char summary for StockHub list card
      bullets:       [str],  # 3-5 bullets (reserved for future detail tab)
      generated_at:  ISODate,
      model:         "qwen-plus",
      source_field:  str,    # which doc field was used as input
      input_chars:   int,
      v:             1,
    }

The reader (`backend/app/api/stock_hub.py::_query_spec`) prefers
``local_ai_summary.tldr`` over the per-source ``preview_field`` chain when it
is non-empty — so populating this field is sufficient to upgrade the card.

Native-summary collections (jinmen.summary_md, gangtise.brief_md when meaningful,
acecamp.summary_md, jiuqian.summary_md) are skipped — their existing
``preview_field`` already produces good cards. The runner only fills gaps.
"""
