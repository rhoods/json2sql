# Features en attente — Optimisation contraintes pass2

## Issue #14 — perf: maintenance_work_mem dynamique
**État :** Spec technique générée (`_bmad-output/feature-constraints-perf-maintenance-work-mem-technical.md`)
**Fichier impacté :** `src/db/ddl.rs` uniquement
**Points clés :**
- Calculer `available_ram` dans `add_constraints` (sysinfo), passer à `apply_constraints_chunk`
- Plancher 256 MB, cap 4 GB
- `synchronous_commit = off` non-fatal
- Timing par table `[CONSTRAINTS] PK <table>: Xms`

À reprendre via `/brain-feature` → continuer issue en cours.
