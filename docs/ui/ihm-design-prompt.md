# Claude Design Prompt — json2sql-ui

Prompt à soumettre à [Claude Design](https://claude.ai/design) pour générer les maquettes de l'IHM.
Le prompt Stitch original a été enrichi pour les stratégies avancées et le file picker cross-platform.

---

Design a desktop developer tool application called "json2sql" — a JSON to PostgreSQL schema migration tool. Dark theme, professional developer aesthetic similar to DBeaver or DataGrip. The app runs on macOS, Windows, and Linux — no external OS dependencies.

Design the following 5 screens as a connected flow:

---

## Screen 1 — Project Setup

Full-screen centered card layout.
- App title "json2sql" with a small database icon
- Section "Source": file picker input for a local JSON/JSONL file. A "Browse…" button opens the native OS file dialog (no external tool required — works on macOS, Windows, Linux). After selection: shows filename, file size (e.g. "2.3 GB"), and a warning banner if > 5 GB ("Large file — analysis may be slow").
- Optional section "Anomaly output": folder picker (same native dialog), label "None" if not set.
- Optional section "Schema snapshot": file picker for a previously saved `.json` schema, with a "Load" button and success indicator.
- Section "Target": PostgreSQL connection form with fields: host, port, database name, username, password, and a "Test connection" button with status indicator (green dot = connected, red = error with message below).
- Advanced options row (collapsed by default): "Workers: [4]" number input, "Parallel pass 2" toggle, "Drop existing tables" toggle.
- Primary CTA button "Start Analysis" at the bottom, disabled until both source file and target connection are configured.

---

## Screen 2 — Schema Analysis (Pass 1)

Split layout: left 60% log panel, right 40% stats panel.
- Header: "Analyzing schema..." with a pulsing indicator
- Left panel: scrollable real-time log output, monospace font, lines like `[12:03:01] Scanned 45,231 records...`, `[12:03:04] Detected table: product (42 columns)`, `[12:03:07] Warning: dynamic keys detected at $.images`
- Right panel: live counters updating in real-time — "Tables detected: 12", "Columns total: 847", "Anomalies: 34", "Records scanned: 45,231 / ~1.2M"
- Bottom: large horizontal progress bar with percentage and estimated time remaining
- Button "Cancel" bottom-left, greyed out primary button "Continue to Schema Review" that activates when done

---

## Screen 3 — Strategy Editor (main workspace)

Three-panel layout: left sidebar (25%), center main panel (45%), right config panel (30%).

**Top bar:** breadcrumb "Setup > Analysis > Strategy Editor", stats row showing "14 tables · 312 columns · 8 anomalies" with colored badge counts.

**Left sidebar — JSON tree:**
Collapsible tree showing the inferred JSON structure. Each node shows:
- Node name + inferred SQL type (grey label)
- Colored strategy badge on the right: blue "default", purple "jsonb", green "flatten", orange "normalize", red "skip"
- Warning badges: red flame icon + number for "wide table" (>100 columns), orange key icon for "dynamic keys detected"
- Currently selected node is highlighted with a distinct background

**Multi-selection interactions:**
- **Click-drag** (hold left mouse button and drag down/up): selects all visible rows swept over, like a list selection. Selected rows show a lighter highlight tint.
- **Ctrl+click**: toggle individual rows in/out of selection.
- **Shift+click**: range-select from last clicked row to current.
- A small floating pill appears above the panel when 2+ rows are selected: "N tables selected — [Clear]"

Example nodes visible:
```
▼ product [default]            ← selected (highlighted)
  ├─ name  VARCHAR             ← selected (highlighted)
  ├─ nutrients  [flatten]      ← selected (highlighted)
  ├─ images  [normalize 🔑 2847 dynamic keys ⚠️]
  └─ ingredients  [jsonb]
```

**Center panel — Table preview:**
Shows the SQL table structure for the currently selected node — column names, types, nullable indicator, and row count estimate. A warning banner at top if anomaly detected: "⚠️ Wide table detected: 2,847 columns. Consider a different strategy."

**Right panel — Strategy configurator:**
Header: "Strategy for $.images"

A vertical list of strategy toggle buttons (one active at a time), each with a colored badge:

| Button | Badge color | 
|---|---|
| Default (columns) | blue |
| JSONB — separate table | purple |
| JSONB — inline column | purple (lighter) |
| Pivot (EAV) | orange |
| Skip (exclude) | red |

Below the buttons: a contextual form section that appears based on the selected strategy.

**Form: "Normalize Dynamic Keys"** (shown when this strategy is selected)
- Label: "Each JSON key becomes a row in a child table."
- Input field: "Key column name" (text, e.g. `image_id`) with validation — must be non-empty and not conflict with existing columns.
- Preview line: `→ Creates table product_images(j2s_parent_id, image_id, value)`
- "Apply Normalize" button, disabled if validation fails.

**Form: "Auto Split"** (shown when this strategy is selected)
- Label: "Split wide table into sub-tables when column count or row size exceeds thresholds."
- Input: "Max columns per table" (number, default 100)
- Input: "Max bytes per row" (number, default 8192)
- "Apply Auto Split" button.

**Form: "Keyed Pivot"** (shown when this strategy is selected)
- Label: "Group rows by a key column and pivot into typed sub-tables."
- Dropdown: "Key column" — populated from the table's actual column list.
- Preview line: `→ Groups rows by [selected_column], creates one sub-table per distinct key.`
- "Apply Keyed Pivot" button.

**Advanced strategies — read-only notice** (shown when auto-detected strategy is StructuredPivot or MultiKeyedPivot)
- Muted italic text: "Auto-detected strategy: Structured Pivot / Multi-Keyed Pivot."
- Small lock icon + label: "Configurable via CLI only — too complex to parameterize in the UI."
- A "Reset to Default" button to override the auto-detected strategy with a simpler one.

**When 2+ tables are selected — multi-select mode in the right panel:**
- Header changes to: "N tables selected"
- Section 1 — **Bulk strategy**: Shows only strategies compatible with bulk-apply (Default, JSONB, Pivot, Skip) as toggle buttons — no contextual forms. A muted note: "Normalize, Auto Split, and Keyed Pivot must be configured per table." "Apply to all N tables" button (primary).
- Section 2 — **Merge tables** (separator line above): A distinct "Merge tables →" button (secondary, outlined). Only enabled when 2+ tables are selected.

**Merge tables — step-by-step panel (replaces the right panel when "Merge tables →" is clicked):**

Step 1 — Similarity analysis (shown immediately, auto-computed):
- Similarity score badge: e.g. "94% compatible" (green) or "61% compatible" (yellow).
- Two sub-cases displayed automatically:
  - **High similarity (>85%)** — "Schemas are nearly identical. Tables can be merged into one with a discriminant column."
    - Input: "Discriminant column name" (text, default `source`) — will hold the original table name as value.
    - Preview: `product_fr + product_en → product (18 cols, +1 col source VARCHAR NOT NULL)`
  - **Partial overlap (<85%)** — "Schemas partially overlap. Non-common columns will be nullable."
    - Read-only list: "12 common columns · 4 nullable (only in images_v1) · 2 nullable (only in images_v2)"
    - Input: "Merged table name" (text, pre-filled with common prefix if detected).
    - Preview: `images_v1 + images_v2 → images (14 cols, 6 nullable)`

Step 2 — Confirm:
- "Apply Merge" button (primary). On confirm: the merged table appears in the left sidebar, the original tables are replaced with a strikethrough + arrow "→ images".
- "Cancel" link to return to multi-select mode.

**Bottom of panel (single-select):** at the very bottom, a muted hint: "Drag in the tree or Ctrl+click to select multiple tables."

**Bottom bar:** "8 anomalies unresolved" warning badge, button "Preview SQL Schema →"

---

## Screen 4 — SQL Schema Preview

Same three-panel layout as Screen 3 but read-only.

**Left sidebar — SQL tree:**
Shows the resulting PostgreSQL schema as a database tree. Tables with column counts, FK relationships shown as indented links with arrow icons.
Example:
```
▼ product (18 cols)
  ├─ product_images (6 cols) ← FK
  └─ product_ingredients (4 cols) ← FK
```

**Center panel — DDL preview:**
Shows the generated `CREATE TABLE` SQL for the selected table, syntax-highlighted, read-only code block.

**Right panel — Diff summary:**
Shows before/after for applied strategies:
- "$.images: normalize_dynamic_keys → -2,847 columns, +1 table (product_images)"
- "$.nutrients: flatten → -1 table, +8 columns in product"

Green for improvements, neutral for unchanged.

**Bottom bar:** buttons "← Back to Strategies" and "Start Import →"

---

## Screen 5 — Import (Pass 2)

Similar layout to Screen 2.
- Header: "Importing data..."
- Left panel: real-time import log, lines like:
  - `[12:15:02] COPY product: 10,000 rows`
  - `[12:15:05] flush product_images (847 rows)`
  - `[12:15:08] Anomaly: VARCHAR overflow at product.description row 12,045 → written to anomalies/product.ndjson`
- Right panel: per-table progress — table name, progress bar, rows imported / total, anomaly count badge
- Bottom: overall progress bar
- On completion: success banner "Import complete — 1,247,832 rows across 14 tables · 42 anomalies logged" with buttons "Open in DBeaver" and "New Import"
