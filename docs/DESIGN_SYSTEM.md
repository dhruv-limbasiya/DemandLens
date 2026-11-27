# DemandLens Design System

> Version 1.0 — Phase 2–4 redesign. Maintained alongside `src/demandlens/app/theme.py`, `static/styles.css`, and `.streamlit/config.toml`.

---

## 1. Design Philosophy

DemandLens is a **statistical-agency publication**, not a SaaS product. The visual language borrows from research terminals and economic data portals: calm, credible, evidence-first. Every element earns its place by communicating data or context — not by decorating the page.

**Core rule:** If a UI element cannot be justified by what it communicates, remove it.

---

## 2. Design Tokens

### 2.1 Colour

| Token | Hex | Usage |
|---|---|---|
| `INK` | `#12161C` | Primary text, interactive controls, `primaryColor` |
| `TEXT_SECONDARY` | `#4B5563` | Labels, section titles, body explanations |
| `TEXT_MUTED` | `#6B7280` | Captions, footnotes, metadata |
| `PAGE_BG` | `#FFFFFF` | Page background |
| `SIDEBAR_BG` | `#F5F6F7` | Sidebar background |
| `BORDER_HAIRLINE` | `#DDE1E6` | Table borders, section dividers |
| `GRIDLINE` | `#E9ECEF` | Chart gridlines |
| `DATA_NEUTRAL` | `#AEB8C3` | Neutral/unemphasized bars |
| **Source: LinkedIn 2024** | `#2B5C8A` | **RESERVED for LinkedIn-only data encoding** |
| **Source: India 2025–26** | `#D9822B` | **RESERVED for India-only data encoding** |

> [!IMPORTANT]
> `#2B5C8A` and `#D9822B` appear **only** on source-encoded charts. They must not be used for UI chrome, buttons, or non-source elements.

**Workplace mode palette:**

| Mode | Hex |
|---|---|
| Remote | `#1F8A7D` |
| Hybrid | `#9FB0C0` |
| Onsite | `#3F4A55` |

**Sequential ramp (choropleth/heatmap):**

`#F3F5F8` → `#C9D4E0` → `#8FA6BF` → `#4F7096` → `#1D3B5A`

### 2.2 Typography

| Property | Value |
|---|---|
| Font family | IBM Plex Sans (self-hosted WOFF2) |
| Weights loaded | 400 (regular), 500 (medium), 600 (semibold) |
| Body size | 14px |
| Line length | 68ch (prose/finding lines) |
| Numeric rendering | `font-variant-numeric: tabular-nums` on all data |

Font files are served from `src/demandlens/app/static/fonts/ibm-plex-sans-{400,500,600}.woff2`.

> [!NOTE]
> Fallback stack: `"IBM Plex Sans", ui-sans-serif, system-ui, sans-serif`.

### 2.3 Spacing

8px base unit. Steps: 4 / 8 / 12 / 16 / 24 / 32 / 48 px.

### 2.4 Border Radii

| Element | Radius |
|---|---|
| Interactive controls (inputs, buttons, selectboxes) | `3px` |
| Panels, expanders, stat strips | `6px` |

---

## 3. Component Catalogue

All components live in `src/demandlens/app/components.py`.

### 3.1 `load_stylesheet()`

Injects `static/styles.css` once per page render via `st.html`. Must be called at app startup in `streamlit_app.py`.

### 3.2 `render_page_header(title, question, extra_scope="")`

Renders:
- `<h1 class="dl-page-title">` — 20px semibold, INK
- `<p class="dl-question">` — the analytical question the page answers, TEXT_SECONDARY
- Right-aligned `st.popover("Data scope")` — snapshot context disclosure

**Always call first in every `render()` function.** The analytical question must be a single sentence that a data analyst would ask.

### 3.3 `render_stat_strip(stats)`

Renders a horizontal hairline-bordered strip of key metrics. Accepts a list of dicts:
```python
{"label": "Top skill (US)", "value": "Python", "note": "32.4%"}
```

- First stat is the **anchor** (double width, 42px value)
- Remaining stats are uniform width, 36px value
- No icons, no background fills, no equal-width KPI cards

### 3.4 `render_section_title(text)`

`<h2 class="dl-section-title">` — 13px uppercase tracking-wide, TEXT_MUTED. Groups a chart and its finding.

### 3.5 `render_finding(sentence)`

`<p class="dl-finding">` — 14px medium, INK, 68ch line length, left border `2px solid #12161C`.

**Rules:**
- One sentence only. Computed from actual data, never hardcoded.
- Uses `labels.build_leader_finding()` wherever possible for consistency.
- Must appear directly above the chart it annotates.

### 3.6 `render_caption(text)`

`<p class="dl-caption">` — 12px, TEXT_MUTED. Methodology notes, disclaimers, min-sample explanations.

### 3.7 `render_empty_state(title, fix_hint)`

Shown when a filtered view returns no data. Provides title + actionable hint (how to change filters).

### 3.8 `render_error_state(message)`

Shown when Gold data fails to load. Uses `st.error()` only for genuine load failures.

### 3.9 `render_underlying_data(df, filename, ...)`

Collapsible expander: `st.dataframe` with `column_config`, formatted columns, CSV download button.

- Percentage columns: `format="%.1f%%"`
- Integer columns: `format="%d"`
- Jaccard columns: `format="%.3f"`
- INR LPA columns: `format="₹%.1f LPA"`

### 3.10 `source_selector(options, key_prefix)`

Persistent snapshot-source selectbox. Stores choice in `st.session_state["selected_source"]`.

### 3.11 `get_gold_build_date()`

Reads actual `_gold_timestamp` from first Parquet file in each `data/gold/*/` directory. Returns the latest date string `YYYY-MM-DD` or `None`. Cached with `@st.cache_data(ttl=3600)`.

### 3.12 `render_footer()`

One-line quiet footer: `Snapshots: LinkedIn 2024 and Indian Job Market 2025–26 · Gold build YYYY-MM-DD`. Build date sourced from real Gold data — never hardcoded.

---

## 4. Chart System

All chart logic lives in `src/demandlens/app/charts.py`.

### 4.1 Global Rules

- **Template:** `plotly_white` base, then `apply_chart_theme()` removes gridlines, sets font to IBM Plex Sans
- **Modebar:** always hidden (`config={"displayModeBar": False}`)
- **Plotly default colors banned:** `#636EFA`, `#EF553B`, `#00CC96` must never appear
- **Colour scales banned:** Plasma, Viridis, Cividis, Rainbow — use the sequential ramp or named palettes
- **Axis lines:** no axis spines; horizontal bars use a faint x-gridline only
- **Legends:** shown only when encoding encodes source identity

### 4.2 `horizontal_ranking_bar(df, label_col, value_col, emphasis_color, ...)`

Standard chart for ranking data. Renders as a horizontal bar chart:
- Top item in `emphasis_color` (source color or INK)
- Remaining items in `DATA_NEUTRAL`
- Y-axis is label (categorical), X-axis is value with optional percent sign
- Optional `highlight_label` to emphasize a non-top item (e.g., selected role)

### 4.3 `workplace_stacked_bar(df, source_col, ...)`

100% stacked horizontal bar for workplace mode (Remote / Hybrid / Onsite). Uses Workplace palette.

### 4.4 `grouped_source_bar(df, category_col, value_col, ...)`

Side-by-side bars for LinkedIn 2024 vs India 2025–26 comparison. Source colors always applied for group identity.

### 4.5 `role_skill_heatmap(pivot_df, height)`

Heatmap of `requirement_frequency_pct` with the sequential ramp. Skills on Y-axis (rows), roles on X-axis (columns). Y sorted by mean frequency descending.

### 4.6 `salary_range_bar(role_summary, height)`

Horizontal range bars showing sample-weighted P25 and P75. **Label: "Sample-weighted cohort P25–P75 range (INR LPA)"**. Rendered via `go.Bar` with error_x for the range. Correctly labeled because values are sample-weighted means of cohort-level P25/P75 figures, not min/max across cohorts.

### 4.7 Skill Co-occurrence Bar

Rendered inline in `skill_cooccurrence.py` using `go.Figure` + `go.Bar`. Bar length = Jaccard similarity; co-occurrence count shown in hover and table only. No second colorbar.

---

## 5. Label Rules

All formatting lives in `src/demandlens/app/labels.py`.

| Function | Input | Output example |
|---|---|---|
| `format_role_name(r)` | `"DATA_ENGINEER"` | `"Data Engineer"` |
| `format_skill_name(s)` | `"python"` | `"Python"` |
| `format_source_name(sid)` | `"linkedin_2024"` | `"LinkedIn 2024"` |
| `format_country_name(cc)` | `"US"` | `"United States"` |
| `format_seniority_name(t)` | `"MID_SENIOR"` | `"Mid-Senior"` |
| `format_column_name(col)` | `"posting_count"` | `"Posting count"` |
| `format_count(n)` | `1348454` | `"1.35M"` |
| `format_pct(p)` | `32.4` | `"32.4%"` |
| `format_inr_lpa(v)` | `1200000` | `"₹12.0 LPA"` |
| `format_jaccard(v)` | `0.342` | `"0.342"` |
| `build_leader_finding(...)` | rank 1 + rank 2 | `"Python leads at 32.4%, ahead of SQL at 28.1%."` |

**Rules:**
- `format_role_name`: replaces `_` with space, title-cases. Known acronyms stay caps: `ML`, `AI`, `BI`, `SQL`, `API`.
- `format_skill_name`: title-cases by default; known proper-cased skills override (e.g., `"python"` → `"Python"`, `"javascript"` → `"JavaScript"`).
- Display labels are computed at render time from raw column names — raw column names are never shown to users.

---

## 6. Navigation Structure

```
DemandLens (st.navigation with groups)
├── Market
│   ├── Overview           /overview     (default)
│   ├── Locations          /locations
│   └── Companies          /companies
├── Skills
│   ├── Skill demand       /skill-demand
│   ├── Role-skill matrix  /role-skill-matrix
│   └── Skill co-occurrence /skill-cooccurrence
└── Outcomes
    ├── Salary             /salary
    └── Career pathways    /career-pathways
```

Dev-only: Kitchen Sink page registered under `Dev` group, only when `DEMANDLENS_DEV_UI=1` env var is set. Never visible in production navigation.

---

## 7. Banned List

The following elements are **strictly forbidden** in DemandLens:

### 7.1 CSS / Visual
- Gradients (linear-gradient, radial-gradient, conic-gradient)
- Glassmorphism (backdrop-filter: blur)
- Box shadows (box-shadow)
- Large border radii (> 6px for data elements)
- Beige or cream background fills
- Italic serif accent text
- Centered hero text
- Chevron arrows as navigation metaphors

### 7.2 Colour
- `#636EFA` (Plotly default blue)
- `#EF553B` (Plotly default red)
- `#00CC96` (Plotly default green)
- Plasma, Viridis, Cividis, Rainbow colour scales
- Purple or indigo anywhere
- `#2B5C8A` outside of LinkedIn-source-encoded data
- `#D9822B` outside of India-source-encoded data

### 7.3 Typography
- Inter, Poppins, Plus Jakarta Sans, Geist fonts
- Emoji characters

### 7.4 Layout Patterns
- Pills (rounded status badges)
- Cards inside cards
- A card wrapping a single number
- Equal-width KPI cards in a grid
- Icon-in-coloured-circle patterns

### 7.5 Streamlit API Misuse
- `st.success()` and `st.info()` as layout furniture (only for genuine alerts)
- Snapshot-banner style blue `EFF6FF`/`2563EB` boxes as decorative chrome
- `page_icon` with an emoji character

---

## 8. Data Disclosure Rules

| Page | Required disclosure |
|---|---|
| All pages | "Data scope" popover in page header with snapshot snapshot names, counts, periods |
| Salary | "Disclosed on ~34% of Indian Job Market 2025–26 postings; LinkedIn 2024 excluded (0% disclosed). Min cohort n = 3." |
| Overview, Skill demand | "Cross-source comparisons use relative prevalence (%)" |
| Career pathways | Entry-level definition per source |

---

## 9. Salary Chart Methodology

**Decision: Sample-weighted cohort P25–P75**

The Gold `salary_analytics_india` table contains cohort-level salary figures keyed by `(standardized_role, seniority_tier, location_clean)`. The columns `salary_min_p25_inr` and `salary_max_p75_inr` are **cohort-level P25/P75 quartiles** (not raw min/max of individual salaries).

When aggregating multiple cohorts, we compute:

$$\hat{P}_{25} = \frac{\sum_i n_i \cdot P_{25,i}}{\sum_i n_i}, \quad \hat{P}_{75} = \frac{\sum_i n_i \cdot P_{75,i}}{\sum_i n_i}$$

where $n_i$ is `sample_size` for cohort $i$. This gives a **sample-weighted mean of cohort quartiles** — a reasonable approximation of the population P25/P75 when cohorts have different sizes. It is correctly labeled **"Sample-weighted cohort P25–P75 range (INR LPA)"**.

The alternative (min of cohort P25s, max of cohort P75s) was rejected because it would inflate the apparent range to the widest outlier cohorts.

---

## 10. Streamlit Version Notes

Verified against **Streamlit 1.67.0**:

| API | Status |
|---|---|
| `st.navigation(groups_dict)` | ✅ Available |
| `st.Page(fn, title, url_path)` | ✅ Available |
| `st.popover(label, width)` | ✅ Available |
| `st.column_config.*` | ✅ Available |
| `st.html(raw_html)` | ✅ Available |
| `AppTest.from_function(fn)` | ✅ Available |
| `AppTest.from_file(path)` | ✅ Available |
| `st.dataframe(row_height=40)` | ✅ Available |
| `plotly_chart(theme=None)` | ✅ Available |
| `config: baseRadius` | ✅ Available |
| `config: buttonRadius` | ✅ Available |
| `config: chartCategoricalColors` | ✅ Available |
| `config: [[theme.fontFaces]]` | ✅ Available |
| `server.enableStaticServing` | ✅ Available |
| `client.toolbarMode` | ✅ Available |

---

## 11. File Map

```
D:\DemandLens\
├── .streamlit/
│   └── config.toml                   # Theme + server config (29 verified keys)
├── static/
│   └── styles.css                    # Single CSS stylesheet (all DL classes)
├── src/demandlens/app/
│   ├── theme.py                      # Python design token constants
│   ├── labels.py                     # All display formatting functions
│   ├── charts.py                     # All chart factory functions
│   ├── components.py                 # All reusable UI components
│   ├── kitchen_sink.py               # Dev-only fixture page (DEMANDLENS_DEV_UI=1)
│   ├── streamlit_app.py              # App entrypoint, st.navigation shell
│   ├── static/fonts/                 # Self-hosted IBM Plex Sans WOFF2 fonts
│   │   ├── ibm-plex-sans-400.woff2
│   │   ├── ibm-plex-sans-500.woff2
│   │   └── ibm-plex-sans-600.woff2
│   └── pages/
│       ├── overview.py
│       ├── locations.py
│       ├── companies.py
│       ├── skill_demand.py
│       ├── role_skill_matrix.py
│       ├── skill_cooccurrence.py
│       ├── salary.py
│       └── career_pathways.py
├── tests/
│   ├── test_app_helpers.py           # Unit tests: labels, formatters, build date
│   ├── test_streamlit_dashboard.py   # AppTest smoke tests: shell + all 8 pages
│   └── test_salary_analytics_filters.py  # Salary cascading filter tests
└── docs/
    └── DESIGN_SYSTEM.md              # This file
```
