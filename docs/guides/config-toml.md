---
type: carte
sujet: Config TOML, overrides de l'IHM et snapshot du schéma
verified_at: 8e10677
files:
  - src/schema/config.rs
  - src/schema/persistence.rs
  - src/schema/strategies.rs
  - src/schema/table_schema.rs
  - src/schema/wide_strategies.rs
  - src/schema/column_groups.rs
  - src/pipeline.rs
  - json2sql-ui/src/state/mod.rs
  - json2sql-ui/src/screens/common/overrides.rs
  - json2sql-ui/src/screens/strategy.rs
  - json2sql-ui/src/screens/setup.rs
  - docs/functional/usage.md
  - docs/technical/schema-inference.md
  - tests/integration_overrides.rs
  - tests/fixtures/
symbols:
  - SchemaConfig
  - SchemaConfig::from_file
  - GroupConfig
  - ConfigWarning
  - ConfigWarning::to_message
  - apply_overrides
  - apply_overrides_complete
  - apply_strategy_override
  - apply_suffix_columns_override
  - suffix_columns_already_applied
  - apply_column_type_overrides
  - apply_group_overrides
  - apply_keyed_pivot_merge
  - rebuilds_columns
  - conflicting_strategy_override
  - DeferredNormalize
  - DeferredFlatten
  - apply_user_overrides
  - apply_single_user_override
  - compute_skip_cascade
  - SkipCascadeWarning
  - prime_tracker_from_pg_type
  - parse_pg_type
  - UserOverride
  - InferredStrategy
  - TableSchema
  - TableSchema::effective_strategy
  - TableSchema::set_toml_override
  - TableSchema::set_ui_override
  - apply_wide_strategy_columns
  - apply_flatten
  - apply_normalize_dynamic_keys
  - apply_jsonb_flatten
  - apply_column_groups
  - dissolve_column_groups
  - exclude_absorbed_children
  - recurse_children
  - already_overridden
  - kept_toml_rebuild
  - absorbed_names
  - StrategyName
  - parse_disabled_strategies
  - SchemaSnapshot
  - format_version
  - SCHEMA_FORMAT_VERSION
  - COLUMN_GROUPS_FORMAT_VERSION
  - write_snapshot
  - save
  - save_with_overrides
  - parse_snapshot
  - load
  - ColumnStats
  - Pass1Result
  - PgType
  - ChildKind
  - JsonFormat
  - run_pipeline
  - load_or_infer_schema
  - restore_from_snapshot
  - post_process_schema
  - apply_schema_config
  - save_schema_snapshot
  - apply_group_columns
  - toml_column_group_tables
  - AppState::load_snapshot
  - build_effective_schemas
  - StrategyScreen
---

# Config TOML, overrides et snapshot

Carte de la zone qui modifie un schéma déjà finalisé (overrides) et qui l'enregistre (snapshot), pour savoir où
intervenir et ce qui casse sans erreur de compilation. Le format du TOML est décrit pour l'utilisateur dans
[usage.md § Config TOML](../functional/usage.md#config-toml---schema-config) et
[schema-inference.md § 12](../technical/schema-inference.md#12-surcharges-manuelles-toml-de-config) ; **ces deux
pages sont en partie fausses** (§ 3). Ajouter une stratégie a sa propre fiche :
[ajouter-une-strategie.md](ajouter-une-strategie.md).

## Rôle

Après la finalisation, deux sources peuvent changer le schéma : le **TOML** de `--schema-config` (CLI seulement :
l'IHM n'en lit jamais) et les **overrides de l'IHM** (`UserOverride` Pivot, Jsonb, Skip, gardés dans le snapshot).
Chaque `TableSchema` garde sa stratégie inférée et un override par source ; `TableSchema::effective_strategy` choisit
dans l'ordre IHM, puis TOML, puis inférée. Le **snapshot** (`SchemaSnapshot`, un JSON) enregistre le résultat de la
Pass 1 pour le recharger sans relire le fichier : `--schema-output` / `--schema-input` en CLI, Save / Load dans l'IHM.

## 1. Où intervenir

| Changement | Où | Le compilateur aide ? |
|---|---|---|
| Nouvelle valeur de `strategy = …` | `apply_strategy_override`, `rebuilds_columns`, `conflicting_strategy_override` (§ 4) | **non** : chaînes, cas par défaut `UnknownStrategy` |
| Nouvelle clé d'une table (comme `prefix`) | sa lecture, et la liste des clés réservées d'`apply_column_type_overrides` (§ 3) | **non** |
| Nouveau type forçable | `parse_pg_type` | **non** : cas par défaut `None` |
| Nouvelle stratégie de groupe (`[group.*]`) | `apply_group_overrides` ; `keyed_pivot` passe par `apply_keyed_pivot_merge` | **non** |
| Nouvel avertissement | une variante de `ConfigWarning`, son message dans `ConfigWarning::to_message`, une ligne au tableau ci-dessous | oui pour le message |
| Nouvel override de l'IHM | `UserOverride`, `apply_single_user_override`, les boutons de `StrategyScreen` | oui pour les `match` sans cas par défaut |
| Nouveau champ du snapshot | `SchemaSnapshot`, ses écrivains et ses lecteurs (§ 4) | oui pour les écrivains, **non** pour les lecteurs |
| Nouvelle version du snapshot | `format_version`, `load` (versions acceptées écrites en dur) | **non** |
| Stratégies désactivables (`--disable-strategy`) | `StrategyName`, `parse_disabled_strategies` ; voir la fiche des stratégies | en partie |

Ce que chaque avertissement laisse de côté. Tous sont des avertissements : rien n'arrête l'import.

| `ConfigWarning` | Déclenché par | Ce qui est ignoré |
|---|---|---|
| `UnknownTable` | une section `[t]` dont le nom n'est pas une table du schéma (nom PostgreSQL, tronqué) | toute la section |
| `UnknownColumn` | une clé non réservée qui n'est pas une colonne | cette clé |
| `UnknownType` | une chaîne que `parse_pg_type` ne reconnaît pas (dont `JSONB`) | ce type |
| `UnknownStrategy` | une valeur de `strategy` inconnue, dont `ignore` | la stratégie |
| `UnknownGroupStrategy` | `[group.g]` avec une autre stratégie que `keyed_pivot` | le groupe |
| `GroupMergeFailed` | moins de deux membres trouvés | la fusion |
| `InvalidOverride` | `columns` ou `suffix_columns` sur une table dont la stratégie inférée n'est pas `Columns` | l'override |
| `AlreadyOverridden` | `strategy` ou `suffix_columns` sur une table qui a déjà un autre override (IHM, ou TOML gardé par le snapshot) ; le même override est ignoré sans avertissement (#129) | `strategy` et `suffix_columns` |
| `ConflictingOverride` | `strategy` (reconnue, autre que `structured_pivot`) avec `suffix_columns` | `strategy` |
| `ArrayColumnOverride` | un type forcé sur une colonne tableau (#84) | ce type |
| `ColumnGroupsMalformed` | `column_groups` qui n'est pas une table de listes | les groupes de la table |
| `ColumnGroupMalformed` | un groupe qui n'est pas une liste de noms | ce groupe |
| `ColumnGroupUnknownColumn` | une colonne absente | ce groupe |
| `ColumnGroupColumnRepeated` | une colonne déjà prise | ce groupe |
| `ColumnGroupGeneratedColumn` | une colonne `j2s_…` | ce groupe |
| `ColumnGroupTooSmall` | moins de colonnes que le minimum | ce groupe |
| `ColumnGroupNameTaken` | un nom de table déjà pris | ce groupe |
| `ColumnGroupParentStrategy` | une table ni `Columns` ni `AutoSplit` | les groupes de la table |
| `ColumnGroupEmptiesParent` | des groupes qui videraient la table | les groupes de la table |

Les overrides de stratégie, qui les pose et ce qu'en fait `apply_single_user_override` (chemin IHM et snapshot) :

| `UserOverride` | Posé par | `apply_single_user_override` |
|---|---|---|
| `Columns` | TOML `strategy = "columns"` (`toml_override`, si la table est inférée `Columns`) | pose `ui_override` si la table est `Columns`, ne change rien d'autre ; refusé si le TOML a déjà reconstruit les colonnes (`kept_toml_rebuild`) |
| `Pivot` | TOML et IHM | reconstruit les colonnes (`apply_wide_strategy_columns`), remet la stratégie inférée ; refusé si le TOML les a déjà reconstruites autrement, gardées si c'est le même |
| `Jsonb` | TOML et IHM | reconstruit les colonnes, même déjà reconstruites par le TOML ; gardées si c'est le même |
| `Skip` | IHM seulement (le TOML refuse `ignore`) | rien : `compute_skip_cascade` retire la table, son compagnon `_wide` et ses enfants |
| `JsonbFlatten` | `apply_jsonb_flatten`, qui n'a pas d'appelant hors tests | **rien** |
| `Flatten` | personne : le TOML `flatten` passe par `apply_flatten`, qui retire l'enfant | **rien** |
| `NormalizeDynamicKeys` | TOML (`toml_override`, après `apply_normalize_dynamic_keys`) | **rien** |

## 2. Flux

```text
CLI : run_pipeline
  load_or_infer_schema ─┬─ Pass 1
                        └─ --schema-input : restore_from_snapshot
                              load (version, migration de strategy_overrides vers ui_override)
                              └─ apply_user_overrides (si strategy_overrides n'est pas vide),
                                   qui finit par exclude_absorbed_children
  post_process_schema
    ├─ apply_schema_config : SchemaConfig::from_file → apply_overrides_complete
    │     ├─ apply_overrides, table par table :
    │     │    already_overridden : une table qui a déjà un override saute strategy et suffix_columns
    │     │    dissolve_column_groups si rebuilds_columns
    │     │    apply_strategy_override (normalize_dynamic_keys et flatten : DeferredNormalize / DeferredFlatten,
    │     │                             appliqués après la boucle sur tout le schéma)
    │     │    apply_suffix_columns_override, apply_column_type_overrides
    │     ├─ apply_group_overrides ([group.*])
    │     ├─ apply_column_groups (column_groups)
    │     └─ exclude_absorbed_children
    ├─ save_schema_snapshot (--schema-output) : le schéma APRÈS le TOML
    └─ apply_group_columns (--group-columns, tables cochées du snapshot ; toml_column_group_tables relit le TOML)

IHM : Pass 1 (AnalysisScreen) ou Load (setup.rs → load → AppState::load_snapshot)
  StrategyScreen : boutons Pivot / Jsonb / Skip → strategy_overrides ; Save → SchemaSnapshot → write_snapshot
  build_effective_schemas → apply_user_overrides sur une copie → le worker reçoit les schémas effectifs
```

## 3. Invariants et pièges

- **Les colonnes ne disent pas la stratégie, seul `effective_strategy` le fait.** Un override Pivot ou Jsonb
  reconstruit les colonnes avec `apply_wide_strategy_columns`, qui écrit aussi `inferred_strategy` ; chaque appelant le
  remet à la main (quatre endroits). Une table pivotée par override a donc des colonnes « key » et « value » et une stratégie
  inférée `Columns`. Les gardes qui lisent `inferred_strategy` (`columns`, `suffix_columns`) la croient encore en
  colonnes : c'est `already_overridden` qui les protège, en passant avant elles.
- **Un override ne reconstruit qu'une fois les colonnes (#129).** Le snapshot de la CLI est écrit *après* le TOML, et
  garde `toml_override` ; `restore_from_snapshot` applique l'IHM avant le TOML. Deux overrides peuvent donc viser la
  même table, et le second partirait des colonnes du premier (avant #129 : `value VARCHAR(2)` devenu `TEXT`, ou
  `suffix_columns` sur une table pivotée, sans avertissement). Côté TOML, `already_overridden` : le même override est
  ignoré sans bruit (snapshot rechargé avec son TOML, usage normal), un autre donne `AlreadyOverridden` (l'IHM gagne,
  comme dans `effective_strategy`). Côté IHM, `kept_toml_rebuild` : sur des colonnes déjà reconstruites par le TOML,
  Pivot et Columns sont refusés (avertissement sur stderr seulement : l'IHM ne l'affiche pas), Jsonb reconstruit.
  Vérifié en `--dry-run` sur les quatre enchaînements. **Limites** : `structured_pivot` / `suffix_columns` et
  `flatten` ne posent pas `toml_override`. Rejouer les mêmes `suffix_columns` sur un snapshot ne change rien
  (`suffix_columns_already_applied`, #163) ; d'autres suffixes donnent `InvalidOverride`. Un `pivot` posé ensuite sur
  la table d'identité n'a pas été revérifié depuis #163. Rejouer `flatten` semble sans effet
  (colonnes dédoublonnées par nom), non testé.
- **Les deux chemins d'override finissent par `exclude_absorbed_children`** : `apply_overrides_complete` (TOML) et
  `apply_user_overrides` (IHM). Une table enfant laissée sous un parent Pivot ou Jsonb reçoit ses données *en plus* du
  parent : la Pass 2 descend dans tout enfant présent dans le schéma (`recurse_children`). C'était le cas de l'IHM
  avant #130 (vérifié sur PostgreSQL, `test_ui_jsonb_on_root_does_not_duplicate_children`). L'IHM montre ces tables
  (`absorbed_names`) : badge « merged » dans Strategy, section « Removed with parent » dans Preview.
- **Des clés TOML ignorées sans avertissement.** Les types forcés se mettent à plat dans `[table]` (`age = "BIGINT"`,
  comme `tests/fixtures/`). La forme `[table.columns]` des deux docs donne une clé `columns` dont la valeur est une
  table : `apply_column_type_overrides` saute toute valeur qui n'est pas une chaîne. Vérifié : aucun override, aucun
  avertissement. De même `age = 5`, un `max_depth` hors de 0..255 (remplacé par 1), un `suffix_columns` sans chaîne.
- **Des noms réservés.** `strategy`, `suffix_columns`, `id_column`, `prefix`, `max_depth` et `column_groups` sont des
  clés de la table : une colonne qui porte un de ces noms ne peut pas être typée. Une table nommée `group` ne peut pas
  être configurée : `[group]` est lu comme les groupes de fusion (erreur de lecture du TOML, vérifié).
- **Les docs annoncent ce que le TOML refuse.** `strategy = "ignore"` donne `UnknownStrategy` (vérifié) ; Skip n'existe
  que dans l'IHM. usage.md ne cite ni `normalize_dynamic_keys`, ni `flatten` (et ses clés `prefix`, `max_depth`,
  `id_column`), ni `[group.*]`.
- **Les noms de stratégie sont des chaînes, en quatre copies** : `apply_strategy_override` (en minuscules, cas par
  défaut `UnknownStrategy`), `rebuilds_columns`, `conflicting_strategy_override` et `already_overridden`. Une valeur
  oubliée dans l'une d'elles ne casse rien à la compilation : la table n'est pas défaite de ses groupes de colonnes, le
  conflit avec `suffix_columns` n'est pas signalé, ou un TOML rechargé avec son snapshot avertit à tort.
- **Ce que le snapshot écrit, il faut pouvoir le relire.** Les noms des variantes de `InferredStrategy`,
  `UserOverride`, `ChildKind`, `JsonFormat` et `PgType` sont écrits dans le fichier : renommer une variante rend les
  anciens snapshots illisibles (précédents : l'alias `Ignore` de `Skip`, `wide_strategy`, le message dédié de
  `parse_snapshot` pour `ScalarArray`). `ColumnStats` n'a pas de `#[serde(default)]` (voir [pass1.md](pass1.md)).
- **Le snapshot est relu champ par champ.** `restore_from_snapshot` le recopie dans un `Pass1Result`,
  `AppState::load_snapshot` dans l'état de l'IHM : un champ oublié compile. C'est déjà le cas dans l'IHM :
  `overflow_warnings` n'est pas relu, la bannière manque après un Load, et un nouveau Save écrit une liste vide.
- **Les overrides de l'IHM sont écrits une seule fois, relus deux fois.** Le Save de l'IHM les met dans
  `strategy_overrides` (racine) ; `load` les recopie dans `ui_override` de chaque table, sans écraser un `ui_override`
  déjà là, puis `apply_user_overrides` les applique.
- **Le parcours du TOML suit une `HashMap`** : l'ordre des tables, donc celui des avertissements, change d'un run à
  l'autre.

## 4. Points d'extension

Les endroits prévus sont listés au § 1. Trois changements demandent des étapes que le compilateur ne rappelle pas.

**Nouvelle valeur de `strategy`.**

- [ ] son cas dans `apply_strategy_override` ; si elle a besoin de tout le schéma, la reporter après la boucle comme
  `DeferredNormalize` ;
- [ ] si elle reconstruit ou déplace des colonnes : l'ajouter à `rebuilds_columns` ; si elle contredit
  `suffix_columns` : à `conflicting_strategy_override` ;
- [ ] décider si elle pose `toml_override` (`TableSchema::set_toml_override`) et si elle remet `inferred_strategy` ;
- [ ] ses clés propres dans la liste réservée d'`apply_column_type_overrides` ;
- [ ] un test dans `config.rs`, puis un test sur un snapshot rechargé avec le même TOML (§ 3, `snapshot_round_trip`) ;
  si elle reconstruit les colonnes, la poser dans `toml_override` et l'ajouter au `match` d'`already_overridden` ;
- [ ] usage.md (« Format général », tableau des stratégies) et schema-inference.md § 12.

**Nouveau champ du snapshot.**

- [ ] le champ dans `SchemaSnapshot` avec `#[serde(default, skip_serializing_if = …)]` : un ancien snapshot se lit
  encore, et un ancien programme ignore le champ. Sans changement de format, pas de nouvelle version (exemple :
  `samples`) ;
- [ ] les quatre écrivains (le compilateur les signale) : `save`, `save_with_overrides`, `save_schema_snapshot` et le
  bouton Save de `StrategyScreen` ;
- [ ] les deux lecteurs (il ne les signale pas) : `restore_from_snapshot` (et `Pass1Result` si besoin),
  `AppState::load_snapshot` ;
- [ ] un test d'aller-retour et un test d'ancien snapshot sans le champ, comme dans `persistence.rs`.

**Nouvelle version du snapshot**, quand un ancien programme lirait le fichier de travers (exemple : les groupes de
colonnes, `COLUMN_GROUPS_FORMAT_VERSION`) :

- [ ] la constante, `format_version` (n'écrire la nouvelle version que si le schéma en a besoin), et les versions
  acceptées par `load`, écrites en dur avec leur message d'erreur.

## 5. Vérifier

```bash
cargo test --lib schema::config
cargo test --lib schema::persistence
cargo test --lib schema::strategies
cargo test --lib pipeline
cargo test --test integration_overrides
cargo test --test guides_consistency
cargo test --lib --bins
```

Un TOML se vérifie sans base avec `--dry-run` : les lignes `Override:` disent ce qui est appliqué, `WARNING:` ce qui est
ignoré, et le DDL le résultat. Pour un piège du § 3, enchaîner `--schema-output` puis `--schema-input` avec le même
TOML.

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées.

1. **Une notion, quatre écritures** (odeurs 1 et 4 de #96, toujours vraies ; #132) : `InferredStrategy`, `UserOverride`,
   `StrategyName`, et les chaînes TOML en quatre copies dans `config.rs` (dont `already_overridden`, #129).
2. **`inferred_strategy` écrit puis remis à la main** (odeur 8 de #96 ; laissé à #132 par #129) :
   `apply_wide_strategy_columns` le modifie, ses appelants le restaurent. Les gardes qui le lisent après un override se
   tromperaient ; `already_overridden` passe avant elles (§ 3).
3. **Le snapshot est relu champ par champ** dans deux lecteurs, dont l'un perd déjà `overflow_warnings` (#133 ; le bug lui-même relève de la carte IHM, #100).
4. **Code sans effet** : `save` et `save_with_overrides` ne servent qu'aux tests (#133) ; `config_warnings` de l'état de
   l'IHM n'est jamais rempli, sa bannière ne s'affiche jamais ; `JsonbFlatten` et `Flatten` ne sont jamais posés
   comme `UserOverride` (odeur 6 de #96).
5. **Docs décalées** (odeur 7 de #96, élargie ; #131) : `[table.columns]` et `ignore` dans usage.md et schema-inference.md ;
   clés absentes d'usage.md ; l'en-tête de `config.rs` parle d'« erreur » pour une table ou une colonne inconnue (ce
   sont des avertissements).
