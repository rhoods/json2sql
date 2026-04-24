# Stitch Design Prompt — json2sql-ui

Prompt à soumettre à [Stitch](https://stitch.withgoogle.com) pour générer les maquettes de l'IHM.

---

Design a desktop developer tool application called "json2sql" — a JSON to PostgreSQL schema migration tool. Dark theme, professional developer aesthetic similar to DBeaver or DataGrip.

Design the following 5 screens as a connected flow:

---

## Screen 1 — Project Setup

Full-screen centered card layout.
- App title "json2sql" with a small database icon
- Section "Source": file picker input for a local JSON/JSONL file, with drag-and-drop zone showing file size and estimated row count after selection
- Section "Target": PostgreSQL connection form with fields: host, port, database name, username, password, and a "Test connection" button with status indicator (green dot = connected)
- Primary CTA button "Start Analysis" at the bottom, disabled until both source and target are configured

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
- Currently selected node is highlighted

Example nodes visible:
```
▼ product [default]
  ├─ name  VARCHAR
  ├─ nutrients  [flatten]
  ├─ images  [normalize 🔑 2847 dynamic keys ⚠️]
  └─ ingredients  [jsonb]
```

**Center panel — Table preview:**
Shows the SQL table structure for the currently selected node — column names, types, nullable indicator, and row count estimate. A warning banner at top if anomaly detected: "⚠️ Wide table detected: 2,847 columns. Consider a different strategy."

**Right panel — Strategy configurator:**
Header: "Strategy for $.images"
Dropdown to select strategy: Default / JSONB / Flatten / Normalize Dynamic Keys / Skip

Below the dropdown: contextual form that changes based on selected strategy.

For "Normalize Dynamic Keys": fields "ID column name: [image_id]", "Prefix: [image_]", preview text "→ Creates table product_images(j2s_parent_id, image_id, url, width, height)"

For "Flatten": fields "Prefix: [nutrients_]", "Max depth: [1]", preview "→ Adds 8 columns to parent table product"

Apply button at bottom of panel.

**Bottom bar:** "8 anomalies unresolved" warning, button "Preview SQL Schema →"

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
