# Architecture IHM — json2sql-ui

Framework : **Dioxus** (Rust, rendu webkit2gtk).  
Binaire : `json2sql-ui/src/main.rs` — injecte le CSS via `theme::design_css()` + `theme::css()` (legacy), monte le composant racine `App`.

---

## Structure des fichiers

```
json2sql-ui/src/
├── main.rs                        # Point d'entrée, injection CSS/JS, routage racine, chargement config
├── config.rs                      # Persistance TOML ~/.config/json2sql/last_project.toml (password exclu)
├── theme.rs                       # Source de vérité : tokens couleur + CSS legacy inline
├── state.rs                       # AppState — conteneur de données pur + constantes PASS1_* + logique sélection
└── screens/
    ├── mod.rs                     # Utilitaires partagés + hook use_elapsed_timer + helpers rfd + tests
    ├── setup.rs                   # Écran 1 — Configuration (wizard accordion + PgConnectionForm)
    ├── analysis.rs                # Écran 2 — Analyse (Pass 1)
    ├── strategy.rs                # Écran 3 — Éditeur de stratégies
    ├── preview.rs                 # Écran 4 — Aperçu SQL
    ├── import.rs                  # Écran 5 — Import (Pass 2)
    ├── table_list.rs              # Composant partagé : TableListPanel (liste tables + sélection)
    └── strategy_configurator.rs   # Composant partagé : panneau config stratégie

json2sql-ui/assets/
└── styles.css                     # Design system CSS (1046 lignes) — chargé via theme::design_css()
```

---

## AppState (`state.rs`)

`AppState` est un **conteneur de données pur**. Il ne contient pas de logique métier — uniquement :
- L'état de navigation (`screen: AppScreen`)
- La config PostgreSQL (`pg: PgConfig`)
- Les résultats de Pass 1 (`schemas`, `pass1_progress`, métadonnées)
- Les overrides utilisateur (`strategy_overrides: HashMap<String, WideStrategy>`)
- L'état de Pass 2 (`pass2_progress`)
- Le handle d'annulation (`abort_handle: Option<tokio::task::AbortHandle>`)

Constantes Pass 1 (publiées ici pour être partagées entre `analysis.rs` et le reste) :
```rust
pub const PASS1_TEXT_THRESHOLD: u32        // nb octets → colonne TEXT
pub const PASS1_WIDE_COLUMN_THRESHOLD: usize  // nb colonnes → table "wide"
pub const PASS1_SIBLING_THRESHOLD: usize   // taille min groupe sibling
pub const PASS1_SIBLING_JACCARD: f64       // seuil Jaccard de similarité
pub const PASS1_STABLE_THRESHOLD: f64      // taux de remplissage "stable"
pub const PASS1_RARE_THRESHOLD: f64        // taux de remplissage "rare"
```

Méthodes de lifecycle :
- `cancel()` — avorte la tâche en cours, remet à zéro l'état transitoire, retourne à Setup
- `apply_progress_event(ProgressEvent)` — applique un événement de progression Pass 1 ou Pass 2
- `load_snapshot()` / `clear_snapshot()` — charge/efface un schéma sauvegardé

Méthodes de sélection (sur `SchemaState`, champ `schema: SchemaState`) :
- `apply_click(i, ctrl)` — clic simple ou Ctrl+clic : sélectionne `i` seul, ou toggle dans la sélection existante
- `apply_shift_click(i, anchor, visible_indices)` — Shift+clic : remplace la sélection par la plage `[anchor..i]` restreinte aux indices visibles
- `apply_sibling_merge(key_col_name)` — appelle `build_keyed_pivot_from_siblings` sur la sélection courante, écrit `KeyedPivot`/`MultiKeyedPivot` sur le parent et `Ignore` sur les siblings absorbés, reset la sélection sur le parent

Fonctions libres :
- `select_children_visible(schemas, parent_idx, visible_indices) -> HashSet<usize>` — retourne les indices des tables dont `parent_table` correspond au nom de la table `parent_idx`, filtrés aux visibles
- `compute_jaccard_display(schemas, indices) -> JaccardDisplay` — score Jaccard min pairwise + ratio colonnes communes/union + `same_parent` pour le panneau Strategy multi-select

La logique de construction de schéma effectif (`build_effective_schemas`) vit dans `screens/mod.rs`.

---

## Persistance config (`config.rs`)

Sauvegarde et rechargement du formulaire Setup entre les sessions.

- Fichier : `~/.config/json2sql/last_project.toml` (créé via `directories-next`)
- **Password exclu** : jamais écrit sur disque
- Chargé au montage dans `App` via `config::load()` → `ProjectConfig::apply_to(&mut state.project)`
- Écrit automatiquement à chaque modification de champ dans `SetupScreen`

---

## Chargement CSS (`main.rs`)

Trois couches injectées dans le `<head>` du webview au démarrage :

1. **Google Fonts** — Inter (UI) + JetBrains Mono (code/data) via `<link>`
2. **Design system** — `theme::design_css()` retourne `include_str!("../assets/styles.css")` — classes layout, tables, badges, logs, etc.
3. **Legacy** — `theme::css()` — variables CSS `:root` + `.btn-primary`, `.btn-ghost`, `.input-field`, `.progress-track`, `.log-panel`, overrides webkit

Un patch JS est également injecté : force `focus()` sur `INPUT`/`TEXTAREA` au `mousedown` (contourne un bug webkit2gtk où la navigation clavier n'est pas routée automatiquement).

---

## Utilitaires partagés (`screens/mod.rs`)

### `use_elapsed_timer<F>(is_done: F) -> Signal<u32>`
Hook partagé : incrémente un compteur de secondes chaque seconde jusqu'à ce que `is_done()` retourne `true`.  
Utilisé par `analysis.rs` (fin de Pass 1) et `import.rs` (fin de Pass 2).  
Contrainte : `F: Fn() -> bool + Clone + 'static` (pas `Send` — `Signal` utilise `UnsyncStorage`).

### `build_effective_schemas(schemas, overrides) -> Vec<TableSchema>`
Applique les overrides utilisateur sur une copie des schémas. Trois passes :
1. Overrides simples (changement de colonnes uniquement)
2. `NormalizeDynamicKeys` (supprime des tables enfants — nécessite la slice complète)
3. `JsonbFlatten` (idem)
4. Suppression des tables `Ignore`

Appelé par `preview.rs`, `import.rs`, et `strategy.rs`.

### `compute_last_child(schemas) -> Vec<bool>`
Pour chaque table, indique si aucune table ultérieure ne partage le même parent.  
Utilisé pour choisir les connecteurs d'arbre (`└─` vs `├─`).

### `build_table_rows(schemas, overrides, overflow_names, selected_indices, filter, show_warn) -> Vec<TableRowViewModel>`
Construit la liste de lignes prête à l'affichage pour `TableListPanel`. Chaque `TableRowViewModel` contient :
- `index`, `name`, `visible`, `indent_px`, `connector` — position et arbre
- `is_selected`, `row_cls` — état sélection
- `badge_cls`, `badge_lbl`, `has_warn`, `is_wide` — indicateurs de stratégie
- `col_count` — nombre de colonnes
- `has_children` — `true` si au moins une table référence cette table comme parent (pré-calculé par `build_table_rows`)

Appelé par `strategy.rs` et `preview.rs`. Doit être appelé **avant** `rsx!` quand des closures capturent `visible_indices`.

### `strategy_label` / `strategy_badge` / `strategy_color`
Mappings `WideStrategy → texte`, `WideStrategy → (css_suffix, short_label)`, `WideStrategy → couleur hex`.  
Source unique pour Strategy, Preview, et StrategyConfigurator.

### Helpers file picker (`rfd`)
`pick_file`, `pick_folder`, `pick_save_file` — dialogues natifs via `rfd::AsyncFileDialog`. Cross-platform, pas de dépendance zenity/GTK externe.

### `progress_pct(done: u64, total: u64) -> u32`
Calcule un pourcentage de progression (0–100). Retourne 0 si `total == 0`, plafonne à 100 si `done >= total`. Partagé entre `analysis.rs` et `import.rs`.

### `ProgressBar` (composant partagé)
Barre de progression labellisée, utilisée par `AnalysisScreen` et `ImportScreen`.

```rust
ProgressBar {
    pct: u32,       // 0–100
    done: bool,     // si true : barre pleine sans animation
    label: String,  // légende sous la barre (bytes, lignes, ETA…)
    phase: String,  // préfixe affiché au-dessus (ex : "A · Streaming")
}
```

---

## Composants partagés

### `StrategyConfigurator` (`strategy_configurator.rs`)

Panneau droit de l'éditeur de stratégies (écran 3 uniquement).

```rust
StrategyConfigurator {
    state: Signal<AppState>,
    idx: usize,                         // index de la table last-selected
    current_strategy: WideStrategy,     // stratégie effective courante
    multi_select: bool,                 // true si >1 table sélectionnée
    selection_count: usize,
    common_parent: Option<String>,
}
```

Deux modes :
- **Single-select** : boucle sur `strategy_options()` → 5 `StrategyButton` + section NormalizeDynamicKeys
- **Multi-select** : boucle sur `strategy_options()` → 5 boutons d'application en masse

Fonctions helper :
- `strategy_options() -> [StrategyOption; 5]` — retourne le tableau des 5 options (Default, JSONB, JSONB inline, Pivot, Skip) avec label et couleur de badge.
- `apply_strategy_override(overrides, name, strategy)` — insère ou supprime un override. Pur, testé unitairement.

Sous-composant privé : `StrategyButton` (bouton toggle actif/inactif avec couleur de badge).

### `PgConnectionForm` (`setup.rs`, composant privé)

Extrait de `SetupScreen` (~145 lignes). Contient tous les champs de connexion PostgreSQL (host, port, database, user, password, schema) + bouton "Test connexion" + affichage du résultat.

```rust
PgConnectionForm { state: Signal<AppState> }
```

Utilisé à l'étape 3 du wizard de `SetupScreen`. Gère ses propres lectures de `state` en local — `SetupScreen` conserve uniquement les signaux utilisés dans les en-têtes d'accordion (`pg_ok`, `pg_testing`, `pg_host`, `pg_db`, `pg_schema`, `schema_invalid`).

### `TableListPanel` (`table_list.rs`)

Composant partagé de liste de tables avec sélection. Utilisé par `strategy.rs` (mode interactif) et `preview.rs` (mode lecture seule).

```rust
TableListPanel {
    rows: Vec<TableRowViewModel>,          // construit par build_table_rows
    show_checkboxes: bool,                 // true = interactif, false = lecture seule
    on_select: EventHandler<(usize, Modifiers)>,
    on_select_children: EventHandler<usize>,
}
```

Deux modes selon `show_checkboxes` :
- **`true` — interactif (Strategy)** : 4 colonnes — checkbox + nom | nb colonnes | badge stratégie | avertissement.  
  Au survol d'une ligne parente (`has_children`), affiche le bouton inline **"⊕ children"** (avec `stop_propagation`).
- **`false` — lecture seule (Preview)** : 2 colonnes — connecteur + nom | badge + avertissement.

Le composant maintient un signal local `hovered: Signal<Option<usize>>` pour l'état de survol.  
Les clics transmettent les `Modifiers` du `MouseEvent` pour permettre Ctrl+clic et Shift+clic au niveau du parent.

---

## Les 5 écrans

### Écran 1 — Setup (`setup.rs`)
Wizard 4 étapes accordion (`.step-card`). Étape 3 délègue à `PgConnectionForm`. Étape 4 (Advanced) est toujours optionnelle.
Sauvegarde la config TOML à chaque modification. Supporte le chargement d'un snapshot JSON sauvegardé.

**Étape 4 — Advanced** :
- **Pass 1 / Pass 2 parallelism** : deux cartes côte à côte avec toggle on/off et champ `Workers`. Pass 1 = workers d'inférence de schéma ; Pass 2 = connexions PG parallèles pour le COPY.
- **Temp directory** : folder picker natif (`rfd`). À la sélection, une probe async (`fs2::available_space`) mesure l'espace disque libre et affiche un badge coloré (vert / jaune / rouge) selon le ratio espace libre / taille du fichier source. Si PostgreSQL tourne localement sur le même disque, une note affiche la réserve totale recommandée (2 × taille source).
- **Table handling** : toggle "Drop existing tables (CASCADE)" avec avertissement destructif.

### Écran 2 — Analysis (`analysis.rs`)
Split 60/40. Panneau gauche : log temps réel (monospace). Panneau droit : 4 tuiles `.stat-tile` (tables, colonnes, lignes, statut).  
Lance Pass 1 en `use_coroutine` au montage. Utilise `use_elapsed_timer` pour le chrono.  
Progress bar + bouton "Continue to Strategy →" activé quand `pass1_progress.done`.

### Écran 3 — Strategy Editor (`strategy.rs`)
Layout trois panneaux `.split-3` (25/45/30) :
- Gauche : `TableListPanel` en mode interactif (`show_checkboxes: true`).
- Centre : liste des colonnes de la table sélectionnée (`table.t`, 2 colonnes : nom / type).
- Droite : `StrategyConfigurator`.

Top bar `.subbar` : breadcrumb + stats + bouton "Save schema" (rfd).

**Sélection de tables** (multi-select implicite — actif dès 2 tables sélectionnées) :
- **Clic simple** — sélectionne la table seule, met à jour l'ancre Shift.
- **Ctrl+clic / ⌘+clic** — toggle la table dans la sélection (ne peut pas désélectionner la dernière).
- **Shift+clic** — sélectionne la plage `[ancre..cible]` sur les lignes **visibles uniquement** (les tables filtrées sont ignorées).
- **Bouton "⊕ children"** (au survol d'une table parente) — sélectionne toutes les tables enfants visibles.
- **Bouton "⊕ all"** (dans la barre de filtre) — sélectionne toutes les tables visibles.

L'ancre Shift est un `Signal<usize>` local à `StrategyScreen` (non persisté dans `AppState`).  
`build_table_rows` est appelé avant `rsx!` pour que `visible_indices: Vec<usize>` soit capturable par les trois handlers.

### Écran 4 — SQL Preview (`preview.rs`)
Layout trois panneaux `.split-3` (25/45/30), read-only :
- Gauche : liste inline des tables (filtre routing containers + tables `Ignore`).
- Centre : DDL généré (`pre.code`) avec tokenizer SQL (`.kw`/`.ty`/`.pn`).
- Droite : résumé de table (nom, stratégie, nb colonnes, table parente, profondeur).

### Écran 5 — Import (`import.rs`)
Split `.split-60-40`. Lance la pipeline complète Pass 2 au montage (connexion PG → DDL → COPY).  
Panneau gauche : log `.log` colorisé. Panneau droit : compteur de lignes par table (`table.t`), triées par volume décroissant.  
Utilise `use_elapsed_timer` pour le chrono. Bannière de succès avec total lignes + anomalies. Bouton "New Import" → `AppState::cancel()`.

**Double barre de progression (bottom bar)** :
Deux composants `ProgressBar` côte à côte (`grid-template-columns: 1fr 1fr`) :
- **A · Streaming** : `bytes_read / total_bytes` — reflète la Phase A (lecture JSON + écriture des fichiers temporaires). Passe à 100 % quand tout le fichier est parsé.
- **B · Inserting** : `tables_done / tables_total` — reflète la Phase B (COPY vers PostgreSQL). Incrémente à chaque `Pass2Flush` reçu. Passe à 100 % à `Pass2Done`.

---

## Flux de navigation

```
Setup ──► Analysis ──► Strategy ──► Preview ──► Import
  ▲                                              │
  └──────────────── cancel() ◄───────────────────┘
```

`cancel()` peut être appelé depuis n'importe quel écran : avorte la tâche en cours et remet à zéro tout l'état transitoire (schemas, progress, overrides). Préserve `source_file` et la config `pg`.

---

## Règles d'architecture

1. **`AppState` = données + sélection.** Pas de logique de transformation de schéma dans `AppState`. Toute logique de construction de schéma effectif va dans `screens/mod.rs`. La logique de sélection UI (`apply_click`, `apply_shift_click`) vit dans `AppState` car elle opère sur `SchemaState` et est testée unitairement sans Dioxus.
2. **Composants = présentation pure.** Les overrides, la détection routing container, les badges overflow sont résolus dans `strategy.rs`, pas dans les composants.
3. **`theme.rs` = source unique des tokens.** Aucune valeur hex dans les composants. Toujours `theme::CONSTANTE`.
4. **`assets/styles.css` = source unique des classes.** Toute nouvelle classe CSS y est définie, pas dans `theme::css()` (legacy).
5. **`screens/mod.rs` = utilitaires partagés.** Tout helper utilisé par ≥2 écrans y vit.
