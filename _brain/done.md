# Features terminées — json2sql

_Archivage des features complétées avec date. Ordre anti-chronologique : la plus récente en premier._

<!-- Format :
## YYYY-MM-DD — Nom de la feature
Description courte de ce qui a été fait.

Ajouter toujours EN HAUT du fichier. -->

## 2026-03-23 — Schema persistence
`--schema-input` / `--schema-output` : sauvegarde et rechargement du snapshot JSON après Pass 1, skip du Pass 1 entièrement si fourni.

## 2026-03-23 — Fix VARCHAR overflow
`coerce()` vérifie la longueur pour `PgType::VarChar(n)` → retourne `Anomaly` au lieu de crasher le COPY.

## 2026-03-23 — Fix NOT NULL violation
NOT NULL uniquement pour colonnes générées (`j2s_id`, `j2s_parent_id`, `j2s_order`), jamais pour les colonnes user-data.

## 2026-03-23 — Fix FK violation flush
Flush de toutes les tables en ordre topologique quand n'importe quelle table atteint le seuil de batch.
