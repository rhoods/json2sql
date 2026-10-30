---
type: recette
sujet: Ajouter une stratégie de matérialisation
verified_at: bd8bd62
files:
  - src/schema/table_schema.rs
  - src/schema/strategies.rs
  - src/schema/finalizer/orchestrator.rs
  - src/schema/finalizer/wide_strategy.rs
  - src/schema/finalizer/guard.rs
  - src/schema/wide_strategies.rs
  - src/schema/cascading/
  - src/schema/config.rs
  - src/schema/persistence.rs
  - src/pipeline.rs
  - src/pass2/insert.rs
  - src/pass2/traversal.rs
  - src/cli.rs
  - json2sql-ui/src/screens/common/strategy_badges.rs
  - json2sql-ui/src/screens/common/overrides.rs
  - json2sql-ui/src/screens/strategy.rs
  - json2sql-ui/src/screens/setup.rs
  - json2sql-ui/src/screens/preview.rs
  - docs/functional/strategies.md
  - docs/functional/usage.md
  - docs/technical/schema-inference.md
  - examples/strategies/
  - tests/integration_strategies.rs
  - tests/header_consistency.rs
symbols:
  - InferredStrategy
  - UserOverride
  - StrategyName
  - StrategyError
  - TableSchema
  - TableSchema::effective_strategy
  - TableSchema::takes_column_groups
  - InferredStrategy::absorbs_children
  - InferredStrategy::is_wide
  - InferredStrategy::from
  - SchemaFinalizer::run
  - finalize_cascading
  - apply_wide_table_strategies
  - apply_wide_strategy
  - apply_non_autosplit_strategy
  - suggest_wide_strategy
  - build_finalizer_config
  - FinalizerConfig
  - apply_column_limit_guard
  - exclude_absorbed_children
  - apply_wide_strategy_columns
  - apply_overrides
  - apply_overrides_complete
  - apply_strategy_override
  - conflicting_strategy_override
  - rebuilds_columns
  - apply_user_overrides
  - apply_single_user_override
  - dissolve_column_groups
  - apply_group_columns
  - build_effective_schemas
  - DeferredFlatten
  - DeferredNormalize
  - format_version
  - COLUMN_GROUPS_FORMAT_VERSION
  - insert_object
  - write_root_jsonb
  - write_autosplit_rows
  - recurse_children
  - insert_child_object
  - insert_array
  - dispatch_child_routes
  - route_independent_child
  - insert_routed_object
  - StrategyName::as_str
  - StrategyName::try_from
  - StrategyName::OPTIONAL
  - strategy_label
  - strategy_badge
  - user_override_badge
  - PreviewScreen
  - StrategyScreen
  - SetupScreen
  - apply_jsonb_flatten
  - apply_pivot_split
  - apply_structured_pivot_split
  - is_pg_array
---

# Ajouter une stratégie

Cette fiche dit **où intervenir** pour ajouter une stratégie. C'est la fiche pilote du format discuté dans #96. Ce que
fait chaque stratégie (le SQL produit, avec et sans)
est dans [strategies.md](../functional/strategies.md), les heuristiques de détection dans
[schema-inference.md](../technical/schema-inference.md).

## Rôle

Une stratégie décide comment une table JSON devient du SQL : une colonne par clé, des lignes clé/valeur, un blob JSONB,
une fusion de tables sœurs… Elle est portée par `TableSchema` (la stratégie inférée et deux overrides possibles) et ne
s'exprime qu'à deux endroits :

- la **forme des colonnes** du schéma. Le DDL (`src/db/`) ignore la stratégie : il crée les colonnes qu'on lui donne ;
- le **routage des valeurs** en Pass 2.

## 1. Quel genre de stratégie ?

Les stratégies existantes, par origine :

| `InferredStrategy` | Détectée automatiquement | Réglable par l'utilisateur | Désactivable (`--disable-strategy`) |
|---|---|---|---|
| `Columns` | par défaut | TOML `columns` | — |
| `Pivot` | phase 3 (`suggest_wide_strategy`) | TOML `pivot`, IHM | `pivot` (donne `Jsonb` à la place) |
| `Jsonb` | phase 3 (valeurs hétérogènes), phase 4 (plus de 1 600 colonnes) | TOML `jsonb`, IHM | — |
| `StructuredPivot` | phase 3 (suffixes) | TOML `suffix_columns` | `structured_pivot` |
| `SiblingCollapse` | phase 2 (`finalize_cascading`) | TOML `[group.*]` | `sibling` |
| `SiblingCollapseMulti` | phase 2 | — | `sibling` |
| `AutoSplit` | phase 3 (racine, fréquences bimodales) | — | non (`split` est obligatoire) |
| `Ignore` | jamais : projection de `UserOverride::Skip` | IHM (`Skip`) | — |
| `NormalizeDynamicKeys` | jamais | TOML `normalize_dynamic_keys` | — |
| `Flatten` | jamais | TOML `flatten` | — |
| `JsonbFlatten` | jamais | aucun chemin trouvé (voir § 6) | — |

Toute nouvelle stratégie passe par le **socle** (§ 4). Selon ses lignes dans ce tableau, ajoute ensuite :

- **A** si elle est détectée automatiquement ;
- **B** si l'utilisateur peut la demander (TOML, IHM) ;
- **C** si `--disable-strategy` peut la couper.

**Hors du tableau : les groupes de colonnes** (`--group-columns`, case « Column groups » de l'IHM, #91). Ce n'est pas
une variante d'`InferredStrategy` : c'est une couche posée après coup, qui découpe une table déjà `Columns` ou
`AutoSplit` en tables 1:1, une par famille de colonnes. La table garde sa stratégie. Une nouvelle stratégie n'a qu'une
décision à prendre de ce côté : `TableSchema::takes_column_groups` (§ 4). Le reste relève de la carte des groupes de
colonnes (#107).

## 2. Flux

```text
Pass 1 (observation)
└─ SchemaFinalizer::run
   ├─ phase 1  schémas de base, table par table
   ├─ phase 2  finalize_cascading           → SiblingCollapse(Multi)               [sibling]
   ├─ phase 3  apply_wide_table_strategies  → Pivot, Jsonb, StructuredPivot, AutoSplit [pivot, structured_pivot]
   └─ phase 4  apply_column_limit_guard (→ Jsonb), puis exclude_absorbed_children
Overrides : TOML (apply_overrides_complete), IHM (apply_user_overrides)
   → TableSchema::effective_strategy : IHM > TOML > inférée
Snapshot (persistence.rs) : schéma et overrides sauvegardés entre Pass 1 et Pass 2
Groupes de colonnes, seulement si TableSchema::takes_column_groups :
   CLI  apply_group_columns, après la sauvegarde du snapshot ; une clé column_groups du TOML l'emporte
   IHM  build_effective_schemas, pour les tables cochées
Pass 2 : insert_object → recurse_children → insert_child_object
         (enfant d'un objet JSONB : le même recurse_children ; tableau d'objets : insert_array)
```

## 3. Invariants et pièges

- **La phase 2 précède la phase 3.** Les tables fusionnables sont fusionnées avant toute stratégie par table ; sinon la
  stratégie vise une table qui va disparaître (table manquante ou double découpage). Voir la doc de `SchemaFinalizer::run`.
- **Deux lectures de la stratégie.** Le champ `inferred_strategy` garde le résultat de l'inférence, même après un
  override (les overrides le restaurent, voir B). `TableSchema::effective_strategy` donne la stratégie appliquée
  (IHM > TOML > inférée) : c'est elle que lisent la Pass 2 et les décisions de schéma (absorption, groupes de colonnes).
  `PreviewScreen` lit `inferred_strategy` exprès, pour montrer l'écart entre l'inférence et le choix de l'utilisateur.
- **Absorption.** Si la stratégie stocke elle-même les données des tables enfants (comme `Pivot` ou `Jsonb`),
  `InferredStrategy::absorbs_children` doit le dire ; sinon `exclude_absorbed_children` garde les tables enfants dans le
  schéma, et le DDL les crée.
- **Snapshot.** `InferredStrategy` et `UserOverride` sont sérialisés tels quels (serde). Une nouvelle variante rend le
  snapshot illisible par un binaire plus ancien. Un renommage casse les anciens snapshots, sauf avec
  `#[serde(alias = …)]` (précédent : `Skip`, alias `Ignore`). Précédent de versionnement : `format_version` n'écrit
  `COLUMN_GROUPS_FORMAT_VERSION` que si la fonctionnalité sert.
- **Les noms de stratégie en chaîne ne sont pas vérifiés par le compilateur** : valeurs TOML, `--disable-strategy`,
  messages d'erreur, docs. Ils sont listés au § 4.
- **Les tests PostgreSQL sont ignorés sans `TEST_DATABASE_URL`** : ils affichent `[SKIP]` et passent au vert. Un run
  vert sans la variable n'a pas testé la Pass 2.

## 4. Recette

### Socle

Ajoute la variante à `InferredStrategy`, puis lance `cargo check --workspace --all-targets`. Le compilateur liste les
`match` sans cas par défaut à compléter (état au commit `verified_at`) :

- `apply_wide_strategy_columns` : forme des colonnes de la table ;
- `insert_child_object` : Pass 2, enfant d'un objet (ordinaire ou JSONB) ;
- `strategy_label`, `strategy_badge` : libellé et badge dans l'IHM.

Les erreurs de l'IHM n'apparaissent qu'une fois la lib corrigée : une erreur dans `json2sql` empêche de compiler
`json2sql-ui`.

Le compilateur ne voit pas le reste :

- [ ] `InferredStrategy::absorbs_children` : la stratégie absorbe-t-elle ses enfants ? (voir § 3)
- [ ] `InferredStrategy::is_wide` : vrai pour tout sauf `Columns` ; vérifier que c'est voulu.
- [ ] `TableSchema::takes_column_groups` : la table peut-elle être découpée en groupes de colonnes ? (aujourd'hui
  `Columns` et `AutoSplit`). `--group-columns`, la case de l'IHM et `--suggest-groups` passent tous par ce test :
  une stratégie oubliée ici n'est jamais découpée, sans avertissement.
- [ ] Pass 2, table racine : `insert_object` traite à part `Jsonb` (`write_root_jsonb`), `SiblingCollapseMulti` et
  `AutoSplit` (`write_autosplit_rows`).
- [ ] Pass 2, tableau d'objets : `insert_array` traite à part `Jsonb`.
- [ ] Pass 2, routes : `insert_routed_object` (appelé par `dispatch_child_routes` et `route_independent_child`), si la
  stratégie fusionne des tables comme `SiblingCollapse`.

### A. Détection automatique

- [ ] Choisir la phase : entre tables, phase 2 (`finalize_cascading`, dossier `cascading/`) ; table par table, phase 3
  (`apply_wide_strategy` et `apply_non_autosplit_strategy` dans `finalizer/wide_strategy.rs`).
- [ ] Construire les colonnes : un cas dans le `match` d'`apply_wide_strategy_columns`, ou une fonction `apply_*` dédiée dans
  `wide_strategies.rs`.
- [ ] Si la stratégie remplace les colonnes de données : garder les colonnes PG array et les vraies tables filles sur
  une table d'identité, en ne déplaçant que les colonnes concernées dans un compagnon (comme `apply_pivot_split` et
  `apply_structured_pivot_split`). Sinon les tableaux et les tables filles disparaissent sans message (#48, #85, #163).
- [ ] Décrire l'heuristique dans `schema-inference.md`.

### B. Choix de l'utilisateur

- [ ] Ajouter la variante à `UserOverride`. Le compilateur signale : `InferredStrategy::from` (projection vers la
  stratégie), `apply_single_user_override`, `user_override_badge`, `PreviewScreen`.
- [ ] TOML : un cas pour la valeur TOML dans le `match` d'`apply_strategy_override`, et la valeur dans les listes de `conflicting_strategy_override` et
  de `rebuilds_columns`. Ce sont trois listes de chaînes à garder alignées.
- [ ] Si l'override réutilise `apply_wide_strategy_columns` : cette fonction écrit aussi `inferred_strategy`. Les cas
  `pivot` et `jsonb` (`apply_strategy_override`, `apply_single_user_override`) sauvegardent la valeur avant l'appel et
  la restaurent après ; faire de même.
- [ ] Si l'application touche d'autres tables que la table visée (comme `flatten`) : passer par le mécanisme différé
  d'`apply_overrides` (`DeferredFlatten`, `DeferredNormalize`).
- [ ] Si la stratégie reconstruit les colonnes : `apply_user_overrides` dissout les groupes de colonnes pour `Pivot` et
  `Jsonb` (`dissolve_column_groups`). Ajouter la variante si besoin.
- [ ] IHM : les boutons de `StrategyScreen`, écrits à la main override par override (actions en masse comprises), et
  la liste `matches!` de la mention « Auto-detected strategy — configurable via CLI only ».
- [ ] Documenter la valeur TOML dans `usage.md` et `strategies.md`.

### C. Désactivable (`--disable-strategy`)

- [ ] Ajouter la variante à `StrategyName`. Le compilateur signale `StrategyName::as_str`.
- [ ] À la main : `StrategyName::OPTIONAL`, `StrategyName::try_from`, les deux messages de `StrategyError`, la doc du
  champ `disable_strategy` dans `src/cli.rs`, la ligne `--disable-strategy` de `usage.md`.
- [ ] Brancher la coupure : un champ de `FinalizerConfig` rempli par `build_finalizer_config` (phase 3), ou un test
  dans `SchemaFinalizer::run` (phase 2, comme `sibling`).
- [ ] IHM : un interrupteur par stratégie, écrit à la main dans `SetupScreen`. La config projet de l'IHM passe déjà par
  `StrategyName::as_str` et `StrategyName::try_from`.

### Toujours : doc et tests

- [ ] `strategies.md` : une section « sans / avec » et ses fichiers dans `examples/strategies/`. Aucun test ne rejoue
  ces exemples : relancer la commande de la section.
- [ ] Le tableau des stratégies du `//!` de `table_schema.rs`, et les blocs `Fonctions :` des fichiers touchés
  (vérifiés par `tests/header_consistency.rs`).
- [ ] Tests unitaires de la détection, dans son module.
- [ ] Un test de bout en bout dans `tests/integration_strategies.rs` : Pass 1, application, Pass 2, puis requêtes sur
  PostgreSQL.

## 5. Vérifier

```bash
cargo check --workspace --all-targets
cargo test --lib --bins
cargo test --test header_consistency
TEST_DATABASE_URL=postgres://user:pass@localhost/testdb cargo test --test integration_strategies
cargo clippy --workspace --all-targets
```

**Revérifier la liste du compilateur** (si la fiche semble périmée). Le faire dans un worktree jetable, jamais dans une
copie de travail qui a des changements en cours : les modifications de la sonde se mélangeraient aux tiennes.

```bash
git worktree add /tmp/j2s-sonde HEAD
```

Dans `/tmp/j2s-sonde`, ajouter une variante factice à `InferredStrategy`, `UserOverride` et `StrategyName`, puis lancer
`cargo check --workspace --all-targets` (avec `CARGO_TARGET_DIR` pointant sur le `target/` du dépôt pour réutiliser les
dépendances compilées). Le premier tour ne montre que les erreurs de la lib. Pour atteindre l'IHM, compléter ces
`match` par `_ => todo!()` et relancer. Une variante de `StrategyName` jamais construite échoue aussi sur
`deny(dead_code)`. Les tests `tc_*` (`--features integration-tests`) ne font aucun `match` sur ces enums. Pour finir :

```bash
git worktree remove --force /tmp/j2s-sonde
```

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées. Chacune peut devenir une issue de refacto.

1. **Trois enums pour une même notion.** `InferredStrategy` (11 variantes), `UserOverride` (7) et `StrategyName` (3),
   plus les chaînes TOML. Une stratégie que l'utilisateur peut demander existe deux fois, reliée par
   `InferredStrategy::from`. Leurs rôles diffèrent (résultat, choix, interrupteur) ; reste à voir si la duplication est
   nécessaire.
2. **Deux répartiteurs presque identiques en Pass 2.** Résolu (#98) : l'enfant d'un objet JSONB passe aussi par
   `recurse_children` et `insert_child_object` ; une nouvelle stratégie ne s'ajoute plus qu'à un `match`.
3. **Des décisions en `matches!`, invisibles au compilateur.** `InferredStrategy::absorbs_children`,
   `TableSchema::takes_column_groups`, `apply_user_overrides` et la mention de `StrategyScreen` classent les
   stratégies par listes : une nouvelle variante y est rangée « non » sans avertissement.
4. **Les noms en chaîne sont recopiés.** Valeurs TOML dans trois fonctions de `config.rs` ; liste des stratégies
   désactivables dans `StrategyError` (deux fois), `src/cli.rs`, `usage.md` et `strategies.md`.
5. **IHM écrite variante par variante.** Les interrupteurs de `SetupScreen` et les boutons de `StrategyScreen` sont
   recopiés pour chaque stratégie au lieu de parcourir `StrategyName::OPTIONAL`.
6. **`JsonbFlatten` semble inatteignable.** Seule `apply_jsonb_flatten` le construit, et seuls des tests l'appellent :
   ni la CLI, ni le TOML, ni l'IHM. Les docs se contredisent (« TOML-only » dans le tableau de `table_schema.rs`, « IHM
   override only » sur la variante de `UserOverride`).
7. **Docs existantes déjà décalées.** `architecture.md` décrit un enum `WideStrategy` (variantes `KeyedPivot`,
   `Ignore`…) qui n'existe plus : c'est `InferredStrategy`. `schema-inference.md` (§ 12) documente
   `strategy = "ignore"`, que le TOML rejette (`--dry-run` affiche « unknown strategy 'ignore' … ignored »).
8. **Un effet de bord à compenser.** `apply_wide_strategy_columns` écrit `inferred_strategy` en plus des colonnes ;
   chaque override qui l'appelle doit sauvegarder puis restaurer la valeur à la main.
