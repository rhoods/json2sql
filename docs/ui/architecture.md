# Architecture IHM — json2sql-ui

Framework : **Dioxus** (Rust, rendu webkit2gtk).  
Binaire : `json2sql-ui/src/main.rs` — injecte le CSS via `theme::css()`, monte le composant racine `App`.

---

## Structure des fichiers

```
json2sql-ui/src/
├── main.rs                        # Point d'entrée, injection CSS, routage racine
├── theme.rs                       # Source de vérité : tokens couleur + CSS
├── state.rs                       # AppState — conteneur de données pur
└── screens/
    ├── mod.rs                     # Utilitaires partagés + tests unitaires
    ├── setup.rs                   # Écran 1 — Configuration
    ├── analysis.rs                # Écran 2 — Analyse (Pass 1)
    ├── strategy.rs                # Écran 3 — Éditeur de stratégies
    ├── preview.rs                 # Écran 4 — Aperçu SQL
    ├── import.rs                  # Écran 5 — Import (Pass 2)
    ├── table_list.rs              # Composant partagé : panneau liste tables
    └── strategy_configurator.rs   # Composant partagé : panneau config stratégie
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

Méthodes de lifecycle :
- `cancel()` — avorte la tâche en cours, remet à zéro l'état transitoire, retourne à Setup
- `apply_progress_event(ProgressEvent)` — applique un événement de progression Pass 1 ou Pass 2
- `load_snapshot()` / `clear_snapshot()` — charge/efface un schéma sauvegardé

La logique de construction de schéma effectif (`build_effective_schemas`) vit dans `screens/mod.rs`, pas dans `AppState`.

---

## Utilitaires partagés (`screens/mod.rs`)

### `build_effective_schemas(schemas, overrides) -> Vec<TableSchema>`
Applique les overrides utilisateur sur une copie des schémas. Trois passes :
1. Overrides simples (changement de colonnes uniquement)
2. `NormalizeDynamicKeys` (supprime des tables enfants — nécessite la slice complète)
3. `JsonbFlatten` (idem)
4. Suppression des tables `Ignore`

Appelé par `preview.rs`, `import.rs`, et `strategy.rs` pour obtenir le schéma effectif à afficher ou importer.

### `compute_last_child(schemas) -> Vec<bool>`
Pour chaque table, indique si aucune table ultérieure ne partage le même parent.  
Utilisé par `TableListPanel` pour choisir les connecteurs d'arbre (`└─` vs `├─`).

### `strategy_label(s) -> &'static str` / `strategy_color(s) -> &'static str`
Mappings `WideStrategy → label texte` et `WideStrategy → couleur badge`.  
Source unique pour Strategy, Preview, et TableListPanel.

### Helpers zenity
`pick_file_zenity`, `pick_folder_zenity`, `pick_save_file_zenity` — ouvrent des dialogues natifs de sélection de fichier. Forcent `GDK_BACKEND=x11` pour éviter les conflits Wayland/GTK.

---

## Composants partagés

### `TableListPanel` (`table_list.rs`)

Panneau gauche hiérarchique, utilisé par **Strategy** et **Preview**.

```rust
TableListPanel {
    entries: Vec<TableRowEntry>,
    on_select: EventHandler<usize>,   // retourne l'index dans schemas
}
```

Le composant est **purement présentationnel** : il ne calcule rien.  
Chaque appelant pré-calcule un `Vec<TableRowEntry>` avec tous les champs d'affichage déjà résolus (badge, couleur, dim, is_selected, connecteur d'arbre).

`TableRowEntry` :
```rust
pub struct TableRowEntry {
    pub index: usize,          // index dans schemas — clé + retour via on_select
    pub name: String,
    pub depth: usize,          // profondeur dans l'arbre JSON
    pub is_last_child: bool,   // connecteur └─ vs ├─
    pub is_selected: bool,     // fond SELECTED_BG + accent vertical
    pub badge_label: &'static str,
    pub badge_color: &'static str,
    pub dim: bool,             // routing containers : opacity 0.6
}
```

### `StrategyConfigurator` (`strategy_configurator.rs`)

Panneau droit de l'éditeur de stratégies (écran 3 uniquement).

```rust
StrategyConfigurator {
    state: Signal<AppState>,
    idx: usize,                         // index de la table last-selected
    current_strategy: WideStrategy,     // stratégie effective courante
    multi_select: bool,                 // true si >1 table sélectionnée
    selection_count: usize,
    common_parent: Option<String>,      // parent commun si multi-select
}
```

Deux modes :
- **Single-select** : boutons de stratégie + section NormalizeDynamicKeys avec champ `id_column`
- **Multi-select** : boutons d'application en masse (Default, JSONB, JSONB inline, Pivot, Skip)

État interne : `normalize_id_col: Signal<String>` — purement local, non partagé.

Sous-composant privé : `StrategyButton` (bouton toggle actif/inactif avec couleur de badge).

---

## Les 5 écrans

### Écran 1 — Setup (`setup.rs`)
Layout centré. Sélection du fichier source (zenity), configuration PostgreSQL, test de connexion, bouton "Start Analysis". Supporte le chargement d'un snapshot JSON sauvegardé.

### Écran 2 — Analysis (`analysis.rs`)
Split 60/40. Panneau gauche : log temps réel (monospace). Panneau droit : compteurs live (tables, colonnes, lignes). Lance Pass 1 en `use_coroutine` au montage. Progress bar + bouton "Continue to Strategy →" activé quand `pass1_progress.done`.

### Écran 3 — Strategy Editor (`strategy.rs`)
Layout trois panneaux (25/45/30) :
- Gauche : `TableListPanel` — arbre des tables avec badges de stratégie, badges overflow ⚠, routing containers atténués. Multi-select via toggle.
- Centre : liste des colonnes de la table sélectionnée (grid 2 colonnes : nom / type).
- Droite : `StrategyConfigurator`.

Top bar : breadcrumb + stats + bouton "Save schema" (zenity).

### Écran 4 — SQL Preview (`preview.rs`)
Layout trois panneaux (25/45/30), read-only :
- Gauche : `TableListPanel` — filtre les routing containers et les tables `Ignore`.
- Centre : DDL généré pour la table sélectionnée (`generate_ddl_preview`).
- Droite : résumé de table (nom, stratégie, nb colonnes, table parente, profondeur).

### Écran 5 — Import (`import.rs`)
Split 60/40. Lance la pipeline complète Pass 2 au montage (connexion PG → DDL → COPY).  
Panneau gauche : log temps réel. Panneau droit : compteur de lignes par table, triées par volume décroissant.  
Bannière de succès avec total lignes + anomalies. Bouton "New Import" → `AppState::cancel()`.

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
2. **Composants = présentation pure.** `TableListPanel` ne calcule rien — le caller pré-calcule `Vec<TableRowEntry>`. Les overrides, la détection routing container, les badges overflow sont résolus dans `strategy.rs`, pas dans le composant.
3. **`theme.rs` = source unique.** Aucune valeur hex dans les composants. Toujours `theme::CONSTANTE`.
4. **`screens/mod.rs` = utilitaires partagés.** Tout helper utilisé par ≥2 écrans y vit.
