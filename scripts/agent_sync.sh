#!/usr/bin/env bash
# agent_sync.sh — crawler multi-agent coordination monitor.
#
# Runs every 30 min via /loop from the coordinating Claude session. Produces a
# machine-readable delta in crawl/.agent_board/ and rewrites §6–§8 of
# crawl/AGENT_COORDINATION.md (everything between the "AUTO-GENERATED: BEGIN"
# and "AUTO-GENERATED: END" markers).
#
# Idempotent — safe to re-run.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BOARD="crawl/AGENT_COORDINATION.md"
STATE_DIR="crawl/.agent_board"
BASELINE_HASH="$STATE_DIR/baseline_hash.txt"
BASELINE_MTIME="$STATE_DIR/baseline_mtime.txt"
BASELINE_TAKEN="$STATE_DIR/baseline_taken_at.txt"
CHANGE_LOG="$STATE_DIR/change_log.tsv"
CURRENT_HASH="$STATE_DIR/current_hash.txt"
CURRENT_MTIME="$STATE_DIR/current_mtime.txt"
LAST_SYNC="$STATE_DIR/last_sync.txt"
GENERATED_FRAGMENT="$STATE_DIR/generated_fragment.md"

mkdir -p "$STATE_DIR"
NOW_ISO="$(date -Iseconds)"
NOW_EPOCH="$(date +%s)"

# ---------- 1. Snapshot current state ----------
find crawl -maxdepth 3 -type f \( -name "*.py" -o -name "*.md" -o -name "*.yaml" \) \
    -not -path "*/logs/*" -not -path "*/__pycache__/*" -not -path "*/pdfs/*" \
    -not -path "*/.claude/*" -not -path "*/.agent_board/*" \
    -exec sha1sum {} \; 2>/dev/null | sort > "$CURRENT_HASH"

find crawl -maxdepth 3 -type f \( -name "*.py" -o -name "*.md" -o -name "*.yaml" -o -name "*.json" \) \
    -not -path "*/logs/*" -not -path "*/__pycache__/*" -not -path "*/pdfs/*" \
    -not -path "*/.claude/*" -not -path "*/.agent_board/*" \
    -printf '%TY-%Tm-%Td %TH:%TM:%.2S  %10s  %p\n' 2>/dev/null | sort > "$CURRENT_MTIME"

# ---------- 2. Compute deltas vs baseline ----------
CHANGED_FILES=""
NEW_FILES=""
DELETED_FILES=""
if [[ -f "$BASELINE_HASH" ]]; then
    # Files whose hash changed OR are new (path appears in current only, or hash differs).
    CHANGED_FILES="$(diff <(cut -c43- "$BASELINE_HASH" | sort) <(cut -c43- "$CURRENT_HASH" | sort) \
        | grep -E "^[<>]" || true)"
    # Hash-level diff (same path, different content).
    MODIFIED="$(comm -13 <(sort "$BASELINE_HASH") <(sort "$CURRENT_HASH") \
        | awk '{print $2}' | sort -u)"
    # Paths only in baseline = deleted. Paths only in current = new.
    BASE_PATHS="$(awk '{print $2}' "$BASELINE_HASH" | sort -u)"
    CUR_PATHS="$(awk '{print $2}' "$CURRENT_HASH" | sort -u)"
    NEW_FILES="$(comm -13 <(echo "$BASE_PATHS") <(echo "$CUR_PATHS"))"
    DELETED_FILES="$(comm -23 <(echo "$BASE_PATHS") <(echo "$CUR_PATHS"))"
    # Modified = in both but hash changed.
    COMMON_PATHS="$(comm -12 <(echo "$BASE_PATHS") <(echo "$CUR_PATHS"))"
    MODIFIED_FILES=""
    if [[ -n "$COMMON_PATHS" ]]; then
        while IFS= read -r p; do
            bh="$(grep -F "  $p" "$BASELINE_HASH" | awk '{print $1}' | head -1)"
            ch="$(grep -F "  $p" "$CURRENT_HASH" | awk '{print $1}' | head -1)"
            if [[ -n "$bh" && -n "$ch" && "$bh" != "$ch" ]]; then
                MODIFIED_FILES+="$p"$'\n'
            fi
        done <<< "$COMMON_PATHS"
    fi
    MODIFIED_FILES="${MODIFIED_FILES%$'\n'}"
else
    MODIFIED_FILES=""
    NEW_FILES=""
    DELETED_FILES=""
fi

# ---------- 3. Append to change log ----------
{
    if [[ -n "${MODIFIED_FILES:-}" ]]; then
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            printf "%s\tmodified\t%s\n" "$NOW_ISO" "$f"
        done <<< "$MODIFIED_FILES"
    fi
    if [[ -n "${NEW_FILES:-}" ]]; then
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            printf "%s\tnew\t%s\n" "$NOW_ISO" "$f"
        done <<< "$NEW_FILES"
    fi
    if [[ -n "${DELETED_FILES:-}" ]]; then
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            printf "%s\tdeleted\t%s\n" "$NOW_ISO" "$f"
        done <<< "$DELETED_FILES"
    fi
} >> "$CHANGE_LOG"

# ---------- 4. Claim parsing from AGENT_COORDINATION.md §2 ----------
ACTIVE_CLAIMS_RAW="$(awk '
    /^## 2\. Active Claims/ { in_section=1; next }
    /^## 3\./ { in_section=0 }
    in_section { print }
' "$BOARD" 2>/dev/null || true)"

# Extract claim blocks (lines between "### <id>" headings).
CLAIM_OVERLAPS=""
CLAIM_SCOPES="$(echo "$ACTIVE_CLAIMS_RAW" | grep -E '^\- \*\*Scope:\*\*' | sed 's/.*Scope:\*\*[[:space:]]*//' || true)"
# Very simple overlap check: if the same file path appears in two different scope lines, flag it.
if [[ -n "$CLAIM_SCOPES" ]]; then
    DUPLICATE_PATHS="$(echo "$CLAIM_SCOPES" | tr ',' '\n' | tr ' ' '\n' | grep -E '^crawl/' | sort | uniq -d || true)"
    if [[ -n "$DUPLICATE_PATHS" ]]; then
        CLAIM_OVERLAPS="$DUPLICATE_PATHS"
    fi
fi

# Heartbeat staleness: find "Heartbeat:" lines older than 90 min.
STALE_CLAIMS=""
while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    HB_TS="$(echo "$line" | sed -E 's/.*Heartbeat:\*\*[[:space:]]*//; s/[[:space:]]*<!--.*//' )"
    if [[ -n "$HB_TS" ]]; then
        HB_EPOCH="$(date -d "$HB_TS" +%s 2>/dev/null || echo 0)"
        AGE=$(( NOW_EPOCH - HB_EPOCH ))
        if (( HB_EPOCH > 0 && AGE > 5400 )); then
            STALE_CLAIMS+="$line (age: $((AGE/60)) min)"$'\n'
        fi
    fi
done < <(echo "$ACTIVE_CLAIMS_RAW" | grep -E '^\- \*\*Heartbeat:\*\*' || true)

# ---------- 5. Shared-file change detection (no queue entry) ----------
SHARED_FILES=(
    "crawl/antibot.py"
    "crawl/auto_login_common.py"
    "crawl/crawler_monitor.py"
    "crawl/crawler_push.py"
    "crawl/CRAWLERS.md"
    "crawl/README.md"
    "crawl/BOT_USAGE.md"
    "crawl/backfill_6months.py"
)
SHARED_ALERTS=""
for f in "${SHARED_FILES[@]}"; do
    if echo "$MODIFIED_FILES" | grep -qxF "$f" 2>/dev/null; then
        if ! awk '/^## 3\./{s=1;next} /^## 4\./{s=0} s' "$BOARD" 2>/dev/null | grep -qF "$f"; then
            SHARED_ALERTS+="- \`$f\` changed without a §3 queue entry"$'\n'
        fi
    fi
done

# ---------- 6. Git status snapshot (crawl/ scope) ----------
GIT_CRAWL="$(git status --short -- crawl/ 2>/dev/null | head -40 || true)"
UNTRACKED_CRAWL_COUNT="$(git status --short -- crawl/ 2>/dev/null | grep -c '^??' || true)"; UNTRACKED_CRAWL_COUNT=${UNTRACKED_CRAWL_COUNT:-0}
MODIFIED_CRAWL_COUNT="$(git status --short -- crawl/ 2>/dev/null | grep -c '^ M\|^M ' || true)"; MODIFIED_CRAWL_COUNT=${MODIFIED_CRAWL_COUNT:-0}

# ---------- 7. Build the AUTO-GENERATED fragment ----------
{
    echo "<!-- AUTO-GENERATED: BEGIN (do not edit between these markers — rewritten by scripts/agent_sync.sh) -->"
    echo ""
    echo "## 6. Recent Changes — AUTO-GENERATED"
    echo ""
    echo "**Last sync:** $NOW_ISO"
    echo ""
    if [[ -f "$BASELINE_TAKEN" ]]; then
        echo "**Baseline taken at:** $(cat "$BASELINE_TAKEN")"
    fi
    echo ""
    echo "**Changes since baseline** (hash-level, crawl/ only, excluding logs/pdfs/pycache):"
    echo ""
    MOD_COUNT=$(printf '%s\n' "$MODIFIED_FILES" | grep -c . || true); MOD_COUNT=${MOD_COUNT:-0}
    NEW_COUNT=$(printf '%s\n' "$NEW_FILES"      | grep -c . || true); NEW_COUNT=${NEW_COUNT:-0}
    DEL_COUNT=$(printf '%s\n' "$DELETED_FILES"  | grep -c . || true); DEL_COUNT=${DEL_COUNT:-0}
    echo "- Modified: $MOD_COUNT"
    echo "- New:      $NEW_COUNT"
    echo "- Deleted:  $DEL_COUNT"
    echo ""
    if [[ -n "${MODIFIED_FILES:-}" ]]; then
        echo "<details><summary>Modified files</summary>"
        echo ""
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            MT="$(stat -c '%y' "$f" 2>/dev/null | cut -d'.' -f1 || echo "?")"
            echo "- \`$f\`  — mtime $MT"
        done <<< "$MODIFIED_FILES"
        echo ""
        echo "</details>"
        echo ""
    fi
    if [[ -n "${NEW_FILES:-}" ]]; then
        echo "<details><summary>New files</summary>"
        echo ""
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            echo "- \`$f\`"
        done <<< "$NEW_FILES"
        echo ""
        echo "</details>"
        echo ""
    fi
    if [[ -n "${DELETED_FILES:-}" ]]; then
        echo "<details><summary>Deleted files</summary>"
        echo ""
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            echo "- \`$f\`"
        done <<< "$DELETED_FILES"
        echo ""
        echo "</details>"
        echo ""
    fi

    echo "**Git status (crawl/):** $MODIFIED_CRAWL_COUNT modified, $UNTRACKED_CRAWL_COUNT untracked"
    echo ""
    if [[ -n "$GIT_CRAWL" ]]; then
        echo '```'
        echo "$GIT_CRAWL"
        echo '```'
        echo ""
    fi

    echo "---"
    echo ""
    echo "## 7. Conflict Alerts — AUTO-GENERATED"
    echo ""
    if [[ -z "$CLAIM_OVERLAPS" && -z "$STALE_CLAIMS" && -z "$SHARED_ALERTS" ]]; then
        echo "_(no alerts — claims and shared files look clean)_"
    else
        if [[ -n "$CLAIM_OVERLAPS" ]]; then
            echo "**Overlapping claims** (same path claimed by ≥2 agents):"
            echo ""
            echo "$CLAIM_OVERLAPS" | while IFS= read -r p; do
                [[ -z "$p" ]] && continue
                echo "- \`$p\`"
            done
            echo ""
        fi
        if [[ -n "$STALE_CLAIMS" ]]; then
            echo "**Stale claims** (no heartbeat in 90+ min — likely abandoned):"
            echo ""
            echo "$STALE_CLAIMS" | while IFS= read -r line; do
                [[ -z "$line" ]] && continue
                echo "- $line"
            done
            echo ""
        fi
        if [[ -n "$SHARED_ALERTS" ]]; then
            echo "**Shared-file changes without queue entry:**"
            echo ""
            echo "$SHARED_ALERTS"
        fi
    fi
    echo ""
    echo "---"
    echo ""
    echo "## 8. Monitor Health — AUTO-GENERATED"
    echo ""
    echo "| Field | Value |"
    echo "|---|---|"
    echo "| Last sync | $NOW_ISO |"
    echo "| Next expected | ~$(date -d "@$((NOW_EPOCH + 1800))" -Iseconds) |"
    FILE_COUNT="$(wc -l < "$CURRENT_HASH" | tr -d ' ')"
    echo "| Files tracked | $FILE_COUNT |"
    CL_COUNT=$(wc -l < "$CHANGE_LOG" 2>/dev/null | tr -d ' ' || true); CL_COUNT=${CL_COUNT:-0}
    AC_COUNT=$(printf '%s\n' "$ACTIVE_CLAIMS_RAW" | grep -cE '^### ' || true); AC_COUNT=${AC_COUNT:-0}
    echo "| Change-log entries | $CL_COUNT |"
    echo "| Active claims | $AC_COUNT |"
    echo ""
    echo "<!-- AUTO-GENERATED: END -->"
} > "$GENERATED_FRAGMENT"

# ---------- 8. Splice fragment into the board ----------
python3 - "$BOARD" "$GENERATED_FRAGMENT" <<'PY'
import pathlib, re, sys
board = pathlib.Path(sys.argv[1])
frag  = pathlib.Path(sys.argv[2]).read_text()
text  = board.read_text()

begin = "<!-- AUTO-GENERATED: BEGIN"
end   = "<!-- AUTO-GENERATED: END -->"

if begin in text and end in text:
    pattern = re.compile(re.escape(begin) + r".*?" + re.escape(end), re.DOTALL)
    new = pattern.sub(frag.rstrip() + "\n", text)
else:
    # First run — replace §6 onward with the fragment.
    idx = text.find("## 6. Recent Changes")
    if idx == -1:
        # Fallback: append.
        new = text.rstrip() + "\n\n" + frag
    else:
        # Keep §9 "How the monitor works" section unchanged (it's static docs).
        how_idx = text.find("## 9. How the monitor works")
        if how_idx != -1:
            new = text[:idx].rstrip() + "\n\n" + frag.rstrip() + "\n\n---\n\n" + text[how_idx:]
        else:
            new = text[:idx].rstrip() + "\n\n" + frag
board.write_text(new)
print(f"wrote {board} ({len(new)} bytes)")
PY

echo "$NOW_ISO" > "$LAST_SYNC"
echo "[agent_sync] ok at $NOW_ISO — modified=$MOD_COUNT new=$NEW_COUNT deleted=$DEL_COUNT"
