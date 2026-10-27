---
type: carte
sujet: IHM, état, écrans, composants, persistance du projet
verified_at: 5dfcced
files:
  - json2sql-ui/src/main.rs
  - json2sql-ui/src/state/
  - json2sql-ui/src/screens/
  - json2sql-ui/src/config.rs
  - json2sql-ui/src/theme.rs
  - json2sql-ui/src/worker_client.rs
  - json2sql-ui/assets/styles.css
  - src/ipc.rs
  - docs/ui/architecture.md
  - docs/ui/design-system.md
  - .github/workflows/ci.yml
symbols:
  - main
  - App
  - AppScreen
  - AppState
  - AppState::cancel
  - AppState::load_snapshot
  - AppState::clear_snapshot
  - AppState::apply_progress_event
  - AppState::apply_worker_result
  - ProjectState
  - SchemaState
  - SchemaState::clear
  - SchemaState::apply_click
  - SchemaState::apply_shift_click
  - ImportState
  - PgConfig
  - ProjectConfig
  - ProjectConfig::from_project
  - ProjectConfig::apply_to
  - ProjectConfig::default
  - load
  - try_save
  - SetupScreen
  - AnalysisScreen
  - StrategyScreen
  - PreviewScreen
  - ImportScreen
  - ResumeScreen
  - TableListPanel
  - ProgressBar
  - use_elapsed_timer
  - build_effective_schemas
  - display_schemas
  - splittable_tables
  - group_preview
  - skip_cascade_names
  - build_table_rows
  - TableRowViewModel
  - RowFlags::compute
  - select_children_visible
  - compute_jaccard_display
  - strategy_label
  - strategy_badge
  - user_override_badge
  - design_css
  - find_active_socket
  - spawn_worker
  - SocketEventReader
  - WorkerKillHandle
  - WorkerConfig
  - WorkerConfig::into_pass2_config
  - Pass1Config
  - RegistryConfig
  - run
  - run_parallel
  - ProgressEvent
  - PASS1_WIDE_COLUMN_THRESHOLD
  - PASS1_SIBLING_THRESHOLD
  - PASS1_SIBLING_JACCARD
  - StrategyName
  - StrategyName::OPTIONAL
  - UserOverride
  - InferredStrategy
---

# IHM : état, écrans, composants

Carte de `json2sql-ui`, pour savoir où intervenir et ce qui casse sans erreur de compilation. Les écrans, les
composants et les règles de rangement sont décrits dans [docs/ui/architecture.md](../ui/architecture.md) (en partie
décalé, § 6) ; les couleurs et les classes CSS dans [docs/ui/design-system.md](../ui/design-system.md). Le dialogue
avec le worker (commande d'annulation, reconnexion, fichier résultat, écran Resume) a sa propre carte, worker et IPC
(#106) : cette fiche n'en garde que ce qui touche l'état de l'IHM.

## Rôle

L'IHM est une application Dioxus de bureau. Tout son état tient dans un seul `AppState`, partagé par un
`Signal<AppState>` que chaque écran reçoit. Elle fait tourner la Pass 1 **dans son propre processus**
(`AnalysisScreen` appelle `run` ou `run_parallel`), puis confie la Pass 2 à un **sous-processus**, `json2sql-worker`
(`ImportScreen` appelle `spawn_worker` avec un `WorkerConfig`). Entre les deux, l'utilisateur choisit des stratégies
(overrides) et des groupes de colonnes, appliqués à une copie des schémas par `build_effective_schemas`.

## 1. Où intervenir

| Changement | Où | Le compilateur aide ? |
|---|---|---|
| Nouvel écran | `AppScreen`, son cas dans `App`, un fichier de `screens/`, les boutons qui y mènent (§ 4) | oui pour le cas dans `App`, non pour la navigation |
| Nouveau réglage du projet, sauvé d'une session à l'autre | `ProjectState`, `ProjectConfig`, `ProjectConfig::apply_to`, un champ dans `SetupScreen` (§ 4) | en partie : pas `ProjectConfig::apply_to` ni `try_save` |
| Nouveau réglage de la Pass 1 | `RegistryConfig` dans `AnalysisScreen` ; les seuils de l'IHM sont des constantes de `state/mod.rs` (§ 3) | oui : `RegistryConfig` est construit en littéral |
| Nouveau paramètre d'import | `WorkerConfig` (`src/ipc.rs`), rempli dans `ImportScreen` ; côté worker, `WorkerConfig::into_pass2_config` | oui : `WorkerConfig` est construit en littéral |
| Nouvelle stratégie ou nouvel override | badges : `strategy_label`, `strategy_badge`, `user_override_badge` ; boutons de `StrategyScreen` ; interrupteurs de `SetupScreen` | oui pour les badges (`match` sans cas par défaut), **non** pour les boutons et interrupteurs |
| Nouvel événement de progression | `ProgressEvent`, `AppState::apply_progress_event` | oui (`match` sans cas par défaut) |
| Nouveau style | une classe dans `assets/styles.css`, chargée par `design_css` | **non** : les classes sont des chaînes |
| Liste de tables, sélection | `build_table_rows` (ce qui s'affiche), `TableListPanel` (l'affichage), `SchemaState::apply_click`, `SchemaState::apply_shift_click`, `select_children_visible` (la sélection) | non : sélection par index (§ 3) |
| Ce que montrent Strategy, Preview et l'import | `build_effective_schemas`, `display_schemas`, `splittable_tables`, `group_preview` | non |

Les écrans, un par variante d'`AppScreen` :

| `AppScreen` | Composant | On y arrive depuis |
|---|---|---|
| `Setup` | `SetupScreen` (`setup.rs`) | le démarrage ; `AppState::cancel` (depuis Analysis, Import, Resume) ; le fil d'Ariane de Strategy et Preview, **sans** `AppState::cancel` |
| `Analysis` | `AnalysisScreen` (`analysis.rs`) | Setup « Start Analysis » ; Strategy « ‹ Analysis » (relance la Pass 1) |
| `Strategy` | `StrategyScreen` (`strategy.rs`) | Analysis « Review schema » ; Setup quand un snapshot est chargé ; Preview |
| `Preview` | `PreviewScreen` (`preview.rs`) | Strategy « Preview SQL schema » ; le fil d'Ariane d'Import |
| `Import` | `ImportScreen` (`import.rs`) | Preview « Start Import » ; Resume « Reprendre » |
| `Resume` | `ResumeScreen` (`resume.rs`) | le démarrage, quand `find_active_socket` trouve un worker encore actif |

## 2. Flux

```text
main : CSS (design_css) + scripts, fenêtre
  └─ App
       ├─ AppState::default, puis load → ProjectConfig::apply_to (le projet sauvé, sans mot de passe)
       ├─ find_active_socket : un worker tourne encore → AppScreen::Resume
       └─ match sur AppScreen (sans cas par défaut) → un écran, qui reçoit le Signal<AppState>

SetupScreen : chaque champ modifié → try_save ; snapshot → AppState::load_snapshot
  └─ AnalysisScreen : Pass 1 dans l'IHM (Pass1Config + RegistryConfig → run / run_parallel)
       ├─ ProgressEvent → AppState::apply_progress_event
       └─ résultat → SchemaState (schemas, échantillons…) ; cases des groupes : splittable_tables
  └─ StrategyScreen : overrides et groupes dans SchemaState ; liste : display_schemas → build_table_rows → TableListPanel
  └─ PreviewScreen : DDL de build_effective_schemas
  └─ ImportScreen : WorkerConfig (schemas = build_effective_schemas) → spawn_worker
       ├─ config sur stdin, mot de passe dans J2S_PG_PASSWORD
       └─ SocketEventReader : ProgressEvent du worker → AppState::apply_progress_event
                              (fin sans événement : AppState::apply_worker_result)

AppState::cancel : arrête la Pass 1 ou le worker (WorkerKillHandle), SchemaState::clear, retour à Setup
```

## 3. Invariants et pièges

**Persistance du projet** (`config.rs`).

- **Un nouveau champ de `ProjectConfig` doit être un `Option` ou porter `#[serde(default)]`.** Sinon, un fichier
  sauvé par une version précédente ne se lit plus : `load` renvoie `None` sans message (`toml::from_str(..).ok()`),
  l'IHM démarre avec les défauts, et le prochain `try_save` réécrit le fichier avec ces défauts. Le projet sauvé est
  perdu, sans erreur.
- **`ProjectConfig::apply_to` affecte les champs un par un.** Un champ oublié là compile : il est sauvé mais jamais
  relu. `ProjectConfig::from_project`, construit en littéral, est signalé par le compilateur.
- **`try_save` est appelé à la main dans chaque handler de `SetupScreen`.** Un champ sans cet appel n'est écrit
  qu'au prochain changement d'un autre champ.
- **`ProjectState` mêle trois sortes de champs que rien ne distingue** : sauvés (dans `ProjectConfig`), transitoires
  (`pg_testing`, `pg_ok`, `pg_error`), et remis à zéro par `AppState::cancel` (`drop_existing`, volontairement).
  `verbose_logs` appelle `try_save` mais n'est pas dans `ProjectConfig` : il n'est jamais sauvé.

**Remises à zéro.**

- **`SchemaState::clear` remet les champs un par un**, pas avec `Default`. Un nouveau champ de `SchemaState` est
  signalé dans `Default` (un littéral), pas dans `SchemaState::clear` : oublié là, il garde la valeur de l'analyse
  précédente après `AppState::cancel` ou `AppState::clear_snapshot`.
- **Changer d'écran ne remet rien à zéro.** Seul `AppState::cancel` le fait. Le fil d'Ariane « Setup » de Strategy
  et de Preview change seulement `screen`. Si l'utilisateur choisit alors un autre fichier et relance l'analyse,
  `AnalysisScreen` remplace les schémas mais garde les overrides, la sélection et les cases des groupes (si
  l'option « Column groups » est décochée). Les overrides, rangés par nom de table, s'appliquent en silence aux
  tables du nouveau fichier qui portent le même nom.

**Sélection par index.** La sélection (`selected_table_indices`, `last_selected_idx`) est une liste de positions,
pas de noms. Or les écrans indexent des listes différentes :

- Strategy indexe `display_schemas` : les tables d'origine, puis les tables de groupe ajoutées à la fin ;
- Preview indexe `build_effective_schemas`, qui retire les tables Skip : la même position y désigne une autre table ;
- `select_children_visible` reçoit les tables d'origine, sans les tables de groupe.

Une position qui sort de la liste fait paniquer l'écran (corrigé pour Strategy dans `e6b5fa6`, en filtrant les
positions trop grandes). Toute nouvelle liste affichée doit dire quelle liste elle indexe.

**Écrit une variante à la fois.**

- Les trois interrupteurs de stratégie de `SetupScreen` sont écrits un par un (`Sibling`, `Pivot`,
  `StructuredPivot`), sans lire `StrategyName::OPTIONAL`. Une nouvelle stratégie désactivable n'apparaît pas dans
  l'IHM, sans erreur.
- Les boutons d'override de `StrategyScreen` (Default, Jsonb, Pivot, Skip) sont écrits un par un, une fois pour
  la table choisie et une fois pour la sélection multiple. Un nouveau `UserOverride` n'y apparaît pas.
- Les décisions en `matches!` valent « faux » pour une variante inconnue : `RowFlags::compute` (badges « ROUTE »
  et « JSONB ⚠ »), la notice « Auto-detected strategy — configurable via CLI only » de `StrategyScreen` (qui ne
  cite pas `SiblingCollapseMulti`).

**Classes CSS.** Une classe est une chaîne dans le code : rien ne vérifie qu'elle existe dans
`assets/styles.css`. Une classe absente donne un élément sans style, sans erreur. Aujourd'hui,
`user_override_badge` renvoie `columns` pour `UserOverride::Columns`, et le bouton « ‹ Analysis » porte
`btn secondary` : aucune des deux n'est définie.

**Hooks Dioxus.** Les `use_signal`, `use_coroutine`… d'un composant doivent être appelés dans le même ordre à chaque
rendu, donc avant tout `return` anticipé. `PreviewScreen` appelle deux `use_signal` après un `return` (liste
vide).

**Deux processus, deux chemins.**

- Un réglage de la Pass 1 va dans `RegistryConfig`, construit dans `AnalysisScreen`. Les seuils de l'IHM sont écrits
  en dur dans `state/mod.rs` et diffèrent des défauts de la CLI (`PASS1_WIDE_COLUMN_THRESHOLD` 10 contre 15,
  `PASS1_SIBLING_THRESHOLD` 3 contre 2, `PASS1_SIBLING_JACCARD` 0,5 contre 0,8) : le même fichier ne donne pas le
  même schéma dans la CLI et dans l'IHM (#112).
- Un paramètre d'import va dans `WorkerConfig`, sérialisé vers le worker. `ImportScreen` lit d'abord tous les champs
  dans un grand tuple, puis construit `WorkerConfig` : le littéral est signalé, le tuple non. Côté worker, un
  nouveau champ de `WorkerConfig` a besoin de `#[serde(default)]` si une IHM et un worker de versions différentes
  peuvent se parler.
- Le worker est cherché à côté de l'exécutable de l'IHM : sans `json2sql-worker` construit, l'import échoue au
  lancement.

**Les tests de l'IHM ne tournent pas en CI.** `cargo test --lib --bins` à la racine ne teste que le paquet
`json2sql`. La CI compile l'IHM avec clippy (`--workspace --all-targets`), sans lancer ses tests. Les lancer à la
main (§ 5).

## 4. Points d'extension

Les endroits prévus sont listés au § 1. Deux changements courants demandent des étapes que le compilateur ne
rappelle pas ; le premier est la recette de #96 « Ajouter un écran ou un panneau à l'IHM ».

**Nouvel écran.**

- [ ] la variante dans `AppScreen` ; le compilateur signale le cas manquant dans `App` ;
- [ ] le composant dans un fichier de `screens/`, son `pub mod` dans `screens/mod.rs` ; tous les hooks avant le
  premier `return` ;
- [ ] les boutons qui y mènent et qui en partent ; pour chacun, décider : `AppState::cancel` (on repart de zéro) ou
  simple changement de `screen` (on garde l'état) ;
- [ ] ce qu'il lit dans `AppState` et ce qu'il y écrit ; un champ nouveau va aussi dans `SchemaState::clear` ;
- [ ] s'il affiche une liste de tables : laquelle (`display_schemas` ou `build_effective_schemas`), et ce que
  désignent alors les positions de la sélection ;
- [ ] ses classes CSS dans `assets/styles.css` ;
- [ ] une ligne dans le tableau des écrans du § 1 (le test `guides_consistency` le vérifie).

**Nouveau réglage du projet, sauvé d'une session à l'autre.**

- [ ] le champ dans `ProjectState` et dans son `Default` ;
- [ ] le champ dans `ProjectConfig`, en `Option` ou avec `#[serde(default)]` (§ 3), et dans `ProjectConfig::default` ;
- [ ] `ProjectConfig::from_project` (le compilateur le signale), puis `ProjectConfig::apply_to` (il ne le signale pas) ;
- [ ] le widget dans `SetupScreen`, avec `try_save` dans son handler ;
- [ ] sa transmission : `RegistryConfig` (Pass 1, `AnalysisScreen`) ou `WorkerConfig` (import, `ImportScreen`) ;
- [ ] décider si `AppState::cancel` le remet à zéro ;
- [ ] un test aller-retour TOML dans `config.rs`, sur le modèle de
  `group_columns_round_trips_toml_and_is_off_for_an_older_file` (il vérifie aussi qu'un ancien fichier se lit).

## 5. Vérifier

```bash
cargo test -p json2sql-ui
cargo clippy -p json2sql-ui --all-targets
cargo check --workspace --all-targets
cargo test --lib --bins
cargo test --test guides_consistency
```

`cargo test -p json2sql-ui` lance les tests de l'état, de la persistance, de la liste de tables et du client du
worker ; aucun ne teste un composant. L'IHM refuse les lints `pedantic` et `nursery` de clippy (`main.rs`). Sous
Linux, la compiler demande les paquets système de Dioxus (webkit2gtk, voir `ci.yml`).

À la main, le worker doit être construit avant l'IHM :

```bash
cargo build -p json2sql-worker
cargo run -p json2sql-ui
```

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées.

1. **Persistance en trois copies.** Un réglage s'écrit dans `ProjectState`, `ProjectConfig` (avec un second
   `Default` qui recopie le premier) et `ProjectConfig::apply_to`, plus un `try_save` dans chaque handler.
   `verbose_logs` n'est pas sauvé ; `temp_dir` est sauvé et relu, mais aucun écran ne le règle et `WorkerConfig`
   ne le transmet pas.
2. **Navigation sans règle de remise à zéro.** Le fil d'Ariane « Setup » fait `AppState::cancel` depuis Analysis et
   Import, mais pas depuis Strategy et Preview. « ‹ Analysis » relance la Pass 1, y compris après un snapshot. Lu
   dans le code, pas reproduit : `resume_socket` reste rempli après un import terminé, donc Import → Preview →
   Import tenterait de se reconnecter à l'ancien worker au lieu d'en lancer un. Voir aussi #70.
3. **`SchemaState::clear` recopie `Default` champ par champ.** `AppState::load_snapshot` ne relit pas
   `overflow_warnings`, pourtant écrit dans le snapshot : les avertissements de débordement disparaissent au
   rechargement.
4. **Deux champs jamais écrits.** `absorbed_names` et `config_warnings` de `SchemaState` sont lus et remis à zéro,
   jamais remplis : le badge « merged » et la bannière des avertissements de config ne peuvent pas s'afficher.
5. **Hooks après un `return` anticipé** dans `PreviewScreen` (§ 3).
6. **Les tests de l'IHM ne tournent pas en CI** (§ 3).
7. **docs/ui/architecture.md décalé** : il décrit `state.rs`, `strategy_configurator.rs`, un enum `WideStrategy`,
   une fonction `theme::css()` et le sélecteur de répertoire temporaire, qui n'existent plus ; `UserOverride` y est
   absent.

Déjà suivi ailleurs : les seuils de la Pass 1 (#112) ; la sélection par index, les boutons et interrupteurs écrits un
par un, la notice « CLI only » et l'ergonomie générale (#94).
