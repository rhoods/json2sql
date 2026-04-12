# Tâches en cours — json2sql

_Mis à jour automatiquement en fin de session._

## Prochaines tâches
- IHM : tester le flow complet sur un fichier réel (Setup → Analysis → Strategy → Preview → Import)
- IHM : `selected_idx` dans AppState pour persister la sélection Strategy ↔ Preview
- IHM : option `--anomaly-dir` exposable (checkbox + dossier) pour streamer les anomalies
- IHM : compteur d'anomalies par table dans le panneau droit de Strategy/Preview
- IHM : barre per-table dans Import = proportion, pas progression — à labelliser ou revoir
- Tester l'import OpenFoodFacts avec l'IHM pour valider le flow à grande échelle

## Contexte de la session précédente

### 2026-04-12 — Migration Leptos → Dioxus + squelette IHM
Migration complète vers Dioxus desktop. Squelette 5 écrans implémenté, design system `theme.rs` créé,
état global `Signal<AppState>` avec `apply_progress_event`. Pas encore de logique câblée dans les écrans.
