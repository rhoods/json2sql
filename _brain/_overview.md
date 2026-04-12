---
project: json2sql
stack: [rust, postgresql, dioxus]
type: cli+desktop
has_sensitive_data: true
deploy_target: local
brain_domains: [rust/error-handling, rust/security-patterns, web/backend/database-patterns]
last_updated: 2026-04-12
---

# json2sql — Contexte

## Description
CLI Rust qui convertit des fichiers JSON/JSONL massifs en base de données PostgreSQL via une inférence de schéma automatique (Pass 1) puis un import COPY optimisé (Pass 2). Une IHM desktop (Dioxus) est en développement pour piloter le workflow complet en 5 écrans.

## Stack
- **Rust** — CLI principal, inférence de schéma, runner COPY
- **PostgreSQL** — base de données cible
- **Dioxus** (desktop) — IHM de pilotage du workflow json2sql

## Domaines cerveau prioritaires
- `rust/error-handling` — gestion d'erreurs robuste sur les gros fichiers
- `rust/security-patterns` — traitement de données JSON arbitraires
- `web/backend/database-patterns` — DDL dynamique, COPY, FK, ordre topologique

## Conventions du projet
- Pass 1 : inférence du schéma (scan complet du fichier)
- Pass 2 : import via COPY PostgreSQL par batch
- Flush en ordre topologique pour respecter les FK
- `j2s_id`, `j2s_parent_id`, `j2s_order` = colonnes générées (seules à avoir NOT NULL)

## IHM Dioxus — Architecture (json2sql-ui)
5 écrans, navigation via `AppScreen` enum, état global `Signal<AppState>` passé en props :
1. **Setup** — source file + config PG (test de connexion)
2. **Analysis** — Pass 1 en cours (progress bar, log, stats tables/colonnes)
3. **Strategy** — éditeur de schéma (badges par stratégie : Default/JSONB/Flatten/Normalize/Skip)
4. **Preview** — aperçu du schéma final avant import
5. **Import / Done** — Pass 2 (progress, log, tableau per-table, banner de fin)

Design system : `theme.rs` — tokens CSS inline, "The Architectural Logic" (voir `docs/DESIGN.md`).
Palette dark high-density, typo Inter + JetBrains Mono, no-line rule (depth par background shifts).

## Points d'attention
- IHM Dioxus : squelette en place, contenu des écrans à implémenter
- Tâche ouverte : compteur d'anomalies par table dans Strategy/Preview
- Les données JSON importées peuvent contenir des données sensibles selon l'usage
