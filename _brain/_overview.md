---
project: json2sql
stack: [rust, postgresql, dioxus]
type: cli+desktop
has_sensitive_data: true
deploy_target: local
brain_domains: [rust/error-handling, rust/security-patterns, web/backend/database-patterns]
last_updated: 2026-06-07
---

# json2sql — Contexte

## Description
CLI Rust qui convertit des fichiers JSON/JSONL massifs en base de données PostgreSQL via une inférence de schéma automatique (Pass 1) puis un import COPY optimisé (Pass 2). Une IHM desktop (Dioxus) pilote le workflow complet en 5 écrans.

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
- Pass 2 : import via COPY PostgreSQL — Phase A streaming pur, Phase B COPY parallèle
- Flush en ordre topologique pour respecter les FK
- `j2s_id`, `j2s_parent_id`, `j2s_order` = colonnes générées (seules à avoir NOT NULL)
- `Pass1Config` struct regroupe tous les paramètres Pass 1 (remplace les params plats)
- `disabled_strategies: HashSet<StrategyName>` — désactivation optionnelle par stratégie

## IHM Dioxus — Architecture (json2sql-ui)
5 écrans, navigation via `AppScreen` enum, état global `Signal<AppState>` passé en props :
1. **Setup** — source file + config PG (test de connexion, temp dir, anomaly dir, parallelism)
2. **Analysis** — Pass 1 en cours (double progress bar, log colorisé, stats tables/colonnes)
3. **Strategy** — éditeur de schéma (badges stratégie, multi-select tables, Jaccard similarity, merge as siblings)
4. **Preview** — aperçu DDL final + diff summary overrides
5. **Import / Done** — Pass 2 (double progress Phase A/B, log colorisé, tableau per-table, banner fin)

Design system : `styles.css` (1046 lignes) + tokens inline. Palette dark high-density, typo Inter + JetBrains Mono.
Persistance config : `~/.config/json2sql/last_project.toml` (password exclu).

## Fonctionnalités actives
- **Optional Strategy Selection** : `--disable-strategy STRATEGY` (CLI) + checkboxes IHM (Setup Advanced)
- **Partial Import / Sample Mode** : import sur sous-ensemble du fichier source
- **Schema persistence** : `--schema-input` / `--schema-output` (skip Pass 1 si fourni)
- **Sibling detection** : threshold 2, clustering glouton, Jaccard pairwise, merge manuel IHM
- **Rust Compiler Constraints** : `clippy.toml` CI gate, `#[must_use]` sur 70 types critiques, 0 unwrap() en prod

## Dette design ouverte
- `_brain/structure_rework.md` — 3 problèmes architecture finalisation schéma (WideStrategy, apply_column_limit_guard, phases implicites)
- **multi-fichiers / NameRegistry** — pour supporter l'analyse multi-fichiers, la `NameRegistry` doit être keyed sur le chemin JSON canonique complet (pas le nom tronqué) ; le nom DDL SQL devient une simple projection. Actuellement l'ordre d'enregistrement détermine qui gagne le nom propre en cas de collision post-Phase-1, ce qui est déterministe en single-file mais cassant si deux fichiers sont traités dans des ordres différents.

## Points d'attention
- Tâche ouverte : compteur d'anomalies par table dans Strategy/Preview
- Tâche ouverte : bouton "Précédent" sur l'écran Strategy
- Les données JSON importées peuvent contenir des données sensibles selon l'usage
