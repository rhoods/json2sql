# Feature : Fix crashes GTK — Save blocking + Pass2 throttling

## Description
Deux crashes GTK indépendants causés par starvation du thread principal. (1) Save : `save_with_overrides` s'exécute en blocking I/O dans un `spawn_local` sans `spawn_blocking`, bloquant le thread GTK — toute interaction UI pendant la sauvegarde (bouger la fenêtre, cliquer) rompt le pipe GDK. (2) Pass2 : `use_coroutine` appelle `state.write()` sur chaque événement `Pass2Progress` (1 par 1 000 lignes), déclenchant des re-renders en cascade qui saturent le thread principal et empêchent GTK de flusher quand l'utilisateur scrolle.

## Motivation
Crash systématique reproductible : fenêtre qui disparaît pendant une sauvegarde schema ou pendant le scroll de la liste de tables en cours d'import sur gros fichier.

## Modules / fichiers impactés
- `json2sql-ui/src/screens/strategy.rs`
- `json2sql-ui/src/screens/import.rs`
- `json2sql-ui/src/screens/mod.rs`
- `src/schema/persistence.rs`

## Tâches de développement
1. `strategy.rs` — wrapper `save_with_overrides` dans `tokio::task::spawn_blocking`
2. `strategy.rs` — ajouter `picking_save: Signal<bool>` guard (re-entrancy, défense en profondeur)
3. `strategy.rs` — reset `save_feedback` à `None` en début de spawn (finding #4 — stale banner)
4. `mod.rs` — déplacer le guard dans `pick_save_file` / `pick_file` / `pick_folder` (finding #5 — altitude)
5. `import.rs` — throttler les `state.write()` à ~10 Hz via `tokio::time::interval`
6. `persistence.rs` — écriture atomique via fichier temporaire + `fs::rename` (finding #6)

## Impacts et edge cases

### Risques de régression identifiés
1. `strategy.rs` — guard `picking_save` : `picking_save.set(false)` doit être hors du `if let PickResult::Selected`, sinon le guard reste bloqué sur cancel (reproduire le pattern exact de setup.rs).
2. `strategy.rs` — spawn_blocking : le `JoinError` (panic closure) doit être attrapé comme branche `Err` supplémentaire en plus du `J2sError`. `J2sError` est bien `Send` (tous variants : `String`, `std::io::Error`, `serde_json::Error`, `tokio_postgres::Error`).
3. `mod.rs` — guard centralisé (static AtomicBool) : les 4 guards de setup.rs restent en place (redondants, inoffensifs). UX change : tous les boutons picker sont grisés dès qu'un seul dialog est ouvert — comportement correct (OS n'autorise qu'un seul dialog natif à la fois).
4. `import.rs` — throttling : utiliser `MissedTickBehavior::Skip` sur l'interval pour éviter un burst de ticks si `rx.recv()` était bloqué >100ms. Sans ça, plusieurs flushes consécutifs seraient déclenchés.
5. `import.rs` — flush résiduel : quand le channel se ferme (`None`), les événements buffurisés depuis le dernier tick doivent être flushés avant le break — sinon les derniers log_lines ("Import complete") n'apparaissent jamais.
6. `persistence.rs` — écriture atomique : `fs::rename` échoue avec `EXDEV` sur cross-filesystem (rare, remonte via `J2sError::Io`). Fichier `.tmp` orphelin possible si kill entre write et rename — `load` l'ignore (lit le chemin original), pas de corruption.

## Documentation
- Spec technique : `_bmad-output/feature-fix-gtk-crashes-technical.md`
