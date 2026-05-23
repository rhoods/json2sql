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
├── state.rs                       # AppState — conteneur de données pur + constantes PASS1_*
└── screens/
    ├── mod.rs                     # Utilitaires partagés + hook use_elapsed_timer + helpers rfd + tests
    ├── setup.rs                   # Écran 1 — Configuration (wizard accordion + PgConnectionForm)
    ├── analysis.rs                # Écran 2 — Analyse (Pass 1)
    ├── strategy.rs                # Écran 3 — Éditeur de stratégies
    ├── preview.rs                 # Écran 4 — Aperçu SQL
    ├── import.rs                  # Écran 5 — Import (Pass 2)
    ├── table_list.rs              # [legacy] TableListPanel — défini mais non utilisé par les écrans actuels
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

### `strategy_label` / `strategy_badge` / `strategy_color`
Mappings `WideStrategy → texte`, `WideStrategy → (css_suffix, short_label)`, `WideStrategy → couleur hex`.  
Source unique pour Strategy, Preview, et StrategyConfigurator.

### Helpers file picker (`rfd`)
`pick_file`, `pick_folder`, `pick_save_file` — dialogues natifs via `rfd::AsyncFileDialog`. Cross-platform, pas de dépendance zenity/GTK externe.

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

### `TableListPanel` (`table_list.rs`) — legacy

Défini mais non utilisé par les écrans actuels : `strategy.rs` et `preview.rs` ont leur propre rendu inline de la liste de tables. Candidat pour une suppression ou une réactivation lors des futurs travaux UX (voir `_brain/ux-todo.md`).

---

## Les 5 écrans

### Écran 1 — Setup (`setup.rs`)
Wizard 4 étapes accordion (`.step-card`). Étape 3 délègue à `PgConnectionForm`.  
Sauvegarde la config TOML à chaque modification. Supporte le chargement d'un snapshot JSON sauvegardé.

### Écran 2 — Analysis (`analysis.rs`)
Split 60/40. Panneau gauche : log temps réel (monospace). Panneau droit : 4 tuiles `.stat-tile` (tables, colonnes, lignes, statut).  
Lance Pass 1 en `use_coroutine` au montage. Utilise `use_elapsed_timer` pour le chrono.  
Progress bar + bouton "Continue to Strategy →" activé quand `pass1_progress.done`.

### Écran 3 — Strategy Editor (`strategy.rs`)
Layout trois panneaux `.split-3` (25/45/30) :
- Gauche : liste inline des tables avec badges de stratégie, connecteurs d'arbre, multi-select.
- Centre : liste des colonnes de la table sélectionnée (`table.t`, 2 colonnes : nom / type).
- Droite : `StrategyConfigurator`.

Top bar `.subbar` : breadcrumb + stats + bouton "Save schema" (rfd).

### Écran 4 — SQL Preview (`preview.rs`)
Layout trois panneaux `.split-3` (25/45/30), read-only :
- Gauche : liste inline des tables (filtre routing containers + tables `Ignore`).
- Centre : DDL généré (`pre.code`) avec tokenizer SQL (`.kw`/`.ty`/`.pn`).
- Droite : résumé de table (nom, stratégie, nb colonnes, table parente, profondeur).

### Écran 5 — Import (`import.rs`)
Split `.split-60-40`. Lance la pipeline complète Pass 2 au montage (connexion PG → DDL → COPY).  
Panneau gauche : log `.log` colorisé. Panneau droit : compteur de lignes par table (`table.t`), triées par volume décroissant.  
Utilise `use_elapsed_timer` pour le chrono. Bannière de succès avec total lignes + anomalies. Bouton "New Import" → `AppState::cancel()`.

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

1. **`AppState` = données pures.** Pas de logique de transformation de schéma dans `AppState`. Toute logique de construction de schéma va dans `screens/mod.rs`.
2. **Composants = présentation pure.** Les overrides, la détection routing container, les badges overflow sont résolus dans `strategy.rs`, pas dans les composants.
3. **`theme.rs` = source unique des tokens.** Aucune valeur hex dans les composants. Toujours `theme::CONSTANTE`.
4. **`assets/styles.css` = source unique des classes.** Toute nouvelle classe CSS y est définie, pas dans `theme::css()` (legacy).
5. **`screens/mod.rs` = utilitaires partagés.** Tout helper utilisé par ≥2 écrans y vit.
