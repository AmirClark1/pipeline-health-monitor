# Pipeline Health Monitor v7.9.3

## Phase 1.5 Production Candidate

This release establishes a stable portfolio baseline before new feature development begins.

### Added

- Floating Operations HUD for operational status and dashboard compute visibility
- Viewer-specific warehouse-metered credit estimation
- Dynamic precision for very small credit values
- Credit classifications: No Usage, Minimal, Light, Normal, and Elevated
- Current-hour estimate handling and freshness messaging
- Runtime slowdown monitoring against successful-run baselines
- Runtime Watchlist and per-pipeline runtime investigation
- Explicit No Current Activity trend classification
- Responsive root-cause presentation
- Redesigned How to Read This Report guide
- Separate Metric Definitions & Limitations reference section

### Improved

- Active failure prioritization and summary messaging
- Recent failure and recovery review workflow
- Stale-pipeline explanation
- Risk, trend, and runtime terminology
- Empty states, report freshness warnings, and operator guidance
- Responsive behavior and UI polish

### Architecture decision

Per-query credit attribution was replaced with a warehouse-metered apportionment estimate. Very short Streamlit queries are not consistently represented in Snowflake query-attribution history, so the application estimates each viewer's share of shared warehouse compute from audited application activity and warehouse metering.

### Security

The public repository must remain scrubbed. Do not commit workplace database names, schemas, roles, warehouses, users, pipeline names, account identifiers, internal alerts, or screenshots containing operational details.

### Status

Production candidate and stable checkpoint for future Phase 2 development.
