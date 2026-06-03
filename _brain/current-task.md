# Tâches en cours — json2sql

_Mis à jour automatiquement en fin de session._

## Sprint actif — Rust Compiler Constraints Refactor (2026-06-02)

Spec : `_bmad-output/specs/rust-compiler-constraints-distillate.md`

### T1 — Clippy Baseline ✅ MESURÉ (ne pas corriger)
- `cargo clippy` défaut : **0 erreurs** — CI gate T2 s'ajoute sans correction préalable
- `cargo clippy -- -D clippy::pedantic` : **551 erreurs**
- Top catégories : `doc_markdown` (198, cosmétique) · `uninlined_format_args` (88, auto-fixable) · `must_use_candidate` (71, → T6) · `missing_errors_doc` (35)
- **Seuils calibrés réels** (remplacent les valeurs initiales du distillate) :
  - `too-many-lines-threshold = 100` (8 fonctions > 100 lignes, max = 528)
  - `cognitive-complexity-threshold = 25` (3 fonctions > 25, max = 57)
  - `too-many-arguments-threshold = 7` (4 fonctions > 7 args)
- Fonctions prioritaires : `cascading.rs::run_sibling_wave` (528L, cplx 57) · `pass2/runner.rs::run` (341L, cplx 42) · `pass2/insert.rs::insert_object` (235L, cplx 41)

### T2 — CI Gate + clippy.toml ✅ LIVRÉ
### T3 — Attributs compilateur ✅ LIVRÉ
### T4 — Éliminer unwrap() en prod ✅ LIVRÉ (11 unwrap → expect)
### T5 — Seuils de complexité (clippy.toml) ✅ LIVRÉ (9 fonctions documentées en debt.md)
### T6 — #[must_use] sur types critiques ✅ LIVRÉ (70 annotations, 22 fichiers)
### T7 — Analyse séparation crates ⬜ DÉFÉRÉ (hors sprint)

## Prochaines tâches

### UX panneau de sélection (voir `_brain/ux-todo.md` pour le détail complet)
- ~~T1 `TableSelectionPanel` : multi-select implicite + Ctrl+clic + Shift+click (visibles) + "⊕ children" + "⊕ all"~~ ✅ livré 2026-05-25
- ~~T2+T3 Jaccard score coloré + Merge as siblings~~ ✅ livré 2026-05-25

### Autres

- IHM : bouton "Précédent" sur l'écran Strategy
- ~~Findings adversariaux mineurs restants : #6 (timing test fragile), #9 (static assert InferredType::ALL.len), #11 (sibling[0] non-déterministe dans large-group Jaccard)~~ ✅ livré 2026-05-25
- Tester Strategy sur des fichiers complexes (wide tables, dynamic keys, pivot)
- Tester import à grande échelle avec OpenFoodFacts + `--anomaly-dir`
- IHM : compteur d'anomalies par table dans Strategy/Preview (nécessite extension du protocole `ProgressEvent` — ajouter `Pass2AnomalyUpdate { table, count }`)
## Backlog (non urgent)

- **SSL/TLS pour connexions PG distantes** : actuellement `NoTls` hardcodé dans `tokio-postgres`. À implémenter si usage cloud (RDS, Supabase, Neon, etc.). Nécessite d'activer `tokio-postgres` avec feature `native-tls` ou `openssl` + dépendance système (`libssl-dev`). Checkbox "Require SSL" dans Setup, propagée au connect dans import.rs et setup.rs.
- Picker de fichier lent (xdg-portal) : installer `libgtk-3-dev` sur le host puis `features = ["tokio", "gtk3"]` dans rfd

## Ce qui est livré (session 2026-05-30)

- **IHM T1** : temp dir picker dans Setup Advanced + probe free space (fs2) + warning vert/jaune/rouge + note PG local
- **IHM T2** : double barre de progression ImportScreen (Phase A Streaming / Phase B Inserting) + composant `ProgressBar` partagé + refacto AnalysisScreen
- **Pass 2 refactoring T1–T5** : séparation franche streaming / COPY — voir `_brain/done.md` pour le détail complet
- `docs/technical/pass2-refactor-plan.md` : document de référence de l'architecture retenue

## Ce qui est livré (session 2026-05-25)

- **T1 multi-sélection tables (Strategy)** : Ctrl+clic toggle, Shift+clic plage sur visibles, bouton "⊕ children" au survol, bouton "⊕ all" dans filter bar
- `TableListPanel` refactorisé en composant partagé (`on_select` passe les `Modifiers`, prop `on_select_children`)
- `apply_click` / `apply_shift_click` / `select_children_visible` testés unitairement dans `state.rs`
- `docs/ui/architecture.md` mis à jour

## Ce qui est livré (session 2026-05-22)

- **F3** : persistance TOML `~/.config/json2sql/last_project.toml` (config.rs, password exclu)
- **E1–E5** : refonte complète des 5 écrans — design system CSS (`.split-3`, `.split-60-40`, `.pane`, `.subbar`, `table.t`, `pre.code`, `.badge`, `.strat-btn`, `.prog`, `.log`, `.stat-tile`, `.step-card`)
- `DdlLine` component + tokenizer SQL pour highlighting DDL (`.kw`, `.ty`, `.pn`)
- Diff summary overrides dans PreviewScreen (from/to strategy badges)
- Log colorization dans ImportScreen (`.warn`, `.err`, `.ok`)
- Zéro erreur Rust `cargo check` sur tous les écrans

## Ce qui est livré (session 2026-05-19)

- **Pass2Flush en temps réel** : `Pass2Flush { table_name, rows_flushed }` émis après chaque COPY réussi dans les conn workers (plus jamais pré-dispatch)
- **Hardening merge_copy_to_db** : remove unwrap → expect avec message, debug_assert total_flushed==0, debug_assert homogénéité table_name, commentaire corrigé (flush_to_db incrémente total_flushed)
- **Tests intégration merge_copy_to_db** : 5 tests (pending-only, spill, multi-sinks, empty-vec, mixed-empty)
- **Test Pass2Flush timing** : `test_pass2_flush_events_emitted_after_copy` — typed event + requête dynamique sur toutes les tables du schéma
- **Fix Pass2Log timing** : suppression des Pass2Log pré-dispatch dans flush_task
- **Suppression test identité mathématique** `per_worker_threshold_bounds_total_disk_usage`

## Ce qui est livré (session 2026-04-25)

- **Perfs Pass 1** : pangenomegraph 23MB 2-3 min → 5-6 sec
  - O(N²) Jaccard → O(N) fast paths (pure-container + large-group)
  - TypeTracker `[u64; 12]` au lieu de IndexMap
  - naming dot-key sans recompute, `ensure_table_key` entry()
  - ObjectArray clone N → N-1
- **Fix j2s_data JSONB** : `is_generated: true` + check `data_col_name` avant `is_generated` en pass2
- **Fix '.' dans les noms de champs JSON** : depth correct, faux commentaire supprimé
- **Fixes adversariaux** : #1 col_sets avant fast path, #2 clone() supprimé, #3 entry(), #7 pop/push, #8 is_generated
- 142 tests passés

## Ce qui est livré (session 2026-04-12)

- 5 écrans Dioxus desktop complets et fonctionnels
- Fixes webkit : inputs éditables (`-webkit-text-fill-color`), focus JS, checkbox restaurée
- Sécurité : host encodé dans URL PG, timeouts 5s/10s sur connect, zeroize sur password
- UX : labels champs PG, compteurs "Detecting…" pendant analyse, port 0 validation, anomaly-dir picker, schéma PG configurable, taille fichier affichée + warning > 5 GB
- Reset progress au remontage écran (mitigation #9)
