# Features terminées — json2sql

_Archivage des features complétées avec date. Ordre anti-chronologique : la plus récente en premier._

<!-- Format :
## YYYY-MM-DD — Nom de la feature
Description courte de ce qui a été fait.

Ajouter toujours EN HAUT du fichier. -->

## 2026-04-12 — IHM Dioxus — implémentation complète
5 écrans entièrement câblés. Setup (file picker rfd + formulaire PG + test connexion),
Analysis (Pass 1 runner via use_coroutine + spawn_blocking), Strategy (éditeur de stratégie
interactif, badges, indentation depth), Preview (DDL généré via generate_create_table),
Import (Pass 2 pipeline complet : connect → DDL → COPY, per-table progress).
Cancel/abort_handle, log cappé 500 lignes, reset état complet.

## 2026-04-12 — IHM Dioxus — squelette
Migration de Leptos vers Dioxus. Squelette complet 5 écrans (Setup, Analysis, Strategy, Preview, Import/Done).
État global via `Signal<AppState>`, design system `theme.rs` aligné sur `docs/DESIGN.md`.
`AppState::apply_progress_event` consomme les `ProgressEvent` du CLI (Pass1/Pass2).

## 2026-03-23 — Schema persistence
`--schema-input` / `--schema-output` : sauvegarde et rechargement du snapshot JSON après Pass 1, skip du Pass 1 entièrement si fourni.

## 2026-03-23 — Fix VARCHAR overflow
`coerce()` vérifie la longueur pour `PgType::VarChar(n)` → retourne `Anomaly` au lieu de crasher le COPY.

## 2026-03-23 — Fix NOT NULL violation
NOT NULL uniquement pour colonnes générées (`j2s_id`, `j2s_parent_id`, `j2s_order`), jamais pour les colonnes user-data.

## 2026-03-23 — Fix FK violation flush
Flush de toutes les tables en ordre topologique quand n'importe quelle table atteint le seuil de batch.
