---
project: json2sql
stack: [rust, postgresql, leptos]
type: cli+desktop
has_sensitive_data: true
deploy_target: local
brain_domains: [rust/error-handling, rust/security-patterns, web/backend/database-patterns]
last_updated: 2026-04-05
---

# json2sql — Contexte

## Description
CLI Rust qui convertit des fichiers JSON/JSONL massifs en base de données PostgreSQL via une inférence de schéma automatique (Pass 1) puis un import COPY optimisé (Pass 2). Une IHM desktop (Leptos) est en cours de développement pour visualiser et explorer le schéma généré.

## Stack
- **Rust** — CLI principal, inférence de schéma, runner COPY
- **PostgreSQL** — base de données cible
- **Leptos** — frontend desktop pour l'IHM de visualisation du schéma

## Domaines cerveau prioritaires
- `rust/error-handling` — gestion d'erreurs robuste sur les gros fichiers
- `rust/security-patterns` — traitement de données JSON arbitraires
- `web/backend/database-patterns` — DDL dynamique, COPY, FK, ordre topologique

## Conventions du projet
- Pass 1 : inférence du schéma (scan complet du fichier)
- Pass 2 : import via COPY PostgreSQL par batch
- Flush en ordre topologique pour respecter les FK
- `j2s_id`, `j2s_parent_id`, `j2s_order` = colonnes générées (seules à avoir NOT NULL)

## Points d'attention
- IHM Leptos : encore bancale, à consolider
- Les données JSON importées peuvent contenir des données sensibles selon l'usage
