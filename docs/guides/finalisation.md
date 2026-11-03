---
type: carte
sujet: Finalisation du schéma, des observations de la Pass 1 aux TableSchema
verified_at: 812f341
files:
  - src/schema/finalizer/
  - src/schema/cascading/
  - src/schema/table_schema.rs
  - src/schema/wide_strategies.rs
  - src/schema/suffix_detector.rs
  - src/schema/naming.rs
  - src/schema/registry.rs
  - src/schema/config.rs
  - docs/technical/schema-inference.md
  - tests/integration_strategies.rs
  - tests/tc_sibling_collapse.rs
  - tests/tc_wide_table.rs
symbols:
  - SchemaRegistry::finalize_with_pg_guard
  - SchemaFinalizer
  - SchemaFinalizer::new
  - SchemaFinalizer::run
  - TableEntry
  - NamingRegistry
  - NamingRegistry::table_name_from_dot_key
  - NamingRegistry::table_name_lookup_from_dot_key
  - ColumnNameRegistry
  - ColumnCollision
  - PG_TABLE_MAX_IDENT
  - TableSchema
  - TableSchema::new
  - ColumnSchema
  - ColumnSchema::generated
  - ColumnSchema::parent_fk
  - InferredStrategy
  - KeyShape
  - ChildKind
  - SiblingSchema
  - SiblingGroup
  - OverflowWarning
  - PG_MAX_COLUMNS
  - StrategyName
  - StrategyName::OPTIONAL
  - FinalizerConfig
  - build_finalizer_config
  - build_base_schemas
  - is_scalar_column
  - finalize_cascading
  - run_sibling_wave
  - drain_cascade_waves
  - run_keyed_pivot_children_wave
  - should_skip_parent
  - handle_single_co_sibling
  - cascade_grandchildren_to_next_wave
  - collect_pivot_co_siblings
  - collect_children_by_key
  - is_multi_pivot
  - reparent
  - reparent_and_update_routes
  - pk_name
  - fk_name
  - build_co_sibling_schema
  - make_subgroup
  - make_pivot_preamble
  - derived_table_name
  - unique_derived_table_name
  - dedup_and_sort
  - unique_cluster_suffix
  - pick_unique_suffix
  - rename_shared_pivots
  - unique_pivot_name
  - classify_key_shape
  - build_union_columns
  - apply_wide_table_strategies
  - apply_wide_strategy
  - apply_non_autosplit_strategy
  - apply_pivot_split
  - apply_autosplit_strategy
  - suggest_wide_strategy
  - detect_suffix_schema
  - apply_wide_strategy_columns
  - apply_column_limit_guard
  - exclude_absorbed_children
  - collect_surviving_route_targets
  - apply_overrides_complete
  - build_sibling_collapse_from_siblings
  - InferredStrategy::absorbs_children
  - TableSchema::takes_column_groups
  - TableSchema::has_order_column
---

# Finalisation du schéma

Carte de la finalisation, pour savoir où intervenir et ce qui casse sans erreur de compilation. Les heuristiques
(tables larges, suffixes, tables sœurs, noms) sont décrites dans [schema-inference.md](../technical/schema-inference.md) ;
ce qui précède (lecture, observation, `SchemaRegistry::finalize_with_pg_guard`) dans la carte [pass1.md](pass1.md) ;
ajouter une stratégie a sa propre fiche : [ajouter-une-strategie.md](ajouter-une-strategie.md).

## Rôle

La finalisation transforme les observations de la Pass 1 (un `TableEntry` par chemin JSON) en une liste de
`TableSchema` triés parents avant enfants : noms PostgreSQL, colonnes typées, colonnes générées (`j2s_…`), stratégie
de chaque table. Elle rend aussi les collisions de noms de colonnes (`ColumnCollision`) et les tables converties par
la garde des 1 600 colonnes (`OverflowWarning`). Tout se passe dans `SchemaFinalizer::run`. `TableSchema`, défini
dans `table_schema.rs`, est le modèle que lisent ensuite les overrides, le DDL, la Pass 2 et l'IHM.

## 1. Où intervenir

| Changement | Où | Le compilateur aide ? |
|---|---|---|
| Nouvelle stratégie | la fiche [ajouter-une-strategie.md](ajouter-une-strategie.md) | en partie |
| Nouveau seuil ou réglage | `SchemaFinalizer::new` (sept arguments par position, voir #112), puis `FinalizerConfig` et `build_finalizer_config` pour la phase 3 | oui pour l'ajout, **non** pour l'ordre des arguments de même type |
| Choix de stratégie d'une table large | `apply_wide_strategy`, `apply_non_autosplit_strategy`, `suggest_wide_strategy`, `detect_suffix_schema` | non |
| Fusion de tables sœurs | `finalize_cascading` et les vagues de `cascading/detection/` | non |
| Forme des clés des tables sœurs | `KeyShape`, `classify_key_shape`, et le nom de colonne clé (§ 4) | **non** |
| Nom d'une table ou d'une colonne | `NamingRegistry`, `ColumnNameRegistry` (`naming.rs`) | non |
| Nouvelle table créée par la finalisation | § 4 | non |
| Garde des 1 600 colonnes | `apply_column_limit_guard` (`PG_MAX_COLUMNS`) | non |
| Quelles tables enfants disparaissent | `exclude_absorbed_children`, `InferredStrategy::absorbs_children` | non |
| Nouveau champ de `TableSchema` | `TableSchema`, `TableSchema::new` (§ 4) | oui pour `TableSchema::new`, non pour les snapshots |
| Fusion manuelle depuis l'IHM | `build_sibling_collapse_from_siblings` (`cascading/merge.rs`) | non |

Les formes de clé des tables sœurs, et la colonne clé qu'elles donnent :

| `KeyShape` | Clés | Colonne clé |
|---|---|---|
| `Numeric` | entiers (`0`, `1`, `42`) | `key_id` |
| `IsoLang` | codes de 2 ou 3 lettres (`fr`, `deu`) | `lang_code` |
| `Slug` | autres chaînes (`en_glass`) | `"key"` |
| `Mixed` | entiers et codes mêlés | `"key"` |

Les genres de table enfant (champ `child_kind`, vide pour la racine) :

| `ChildKind` | Vient de | Conséquences |
|---|---|---|
| `Object` | un objet imbriqué | pas de `j2s_order` |
| `ObjectArray` | un tableau d'objets | une colonne `j2s_order` (`TableSchema::has_order_column`) |

## 2. Flux

```text
SchemaRegistry::finalize_with_pg_guard (voir pass1.md)
└─ SchemaFinalizer::run(tables: TableEntry par chemin, NamingRegistry)
   ├─ pré-enregistre le nom de chaque chemin (NamingRegistry::table_name_from_dot_key)
   ├─ phase 1  build_base_schemas (parallèle) : une TableSchema par TableEntry
   │           colonnes générées, puis colonnes de données (ColumnNameRegistry) ; doublons retirés, tri
   ├─ phase 2  finalize_cascading (sauf --disable-strategy sibling)
   │           run_sibling_wave (vague 0) → drain_cascade_waves (vagues 1+) → vague 0 bis
   │           → run_keyed_pivot_children_wave ; crée des tables synthétiques et des child_routes
   ├─ doublons de nom retirés, tri par profondeur puis chemin JSON
   ├─ phase 3  apply_wide_table_strategies : par TableEntry, si la table est encore Columns
   │           apply_wide_strategy → AutoSplit (racine) ou apply_non_autosplit_strategy
   │           (apply_structured_pivot_split, apply_pivot_split, Jsonb) ; compagnons _wide / _pivot
   │           ajoutés à la fin
   └─ phase 4  apply_column_limit_guard (si apply_pg_guard), puis exclude_absorbed_children
→ (Vec<TableSchema>, Vec<ColumnCollision>, Vec<OverflowWarning>)
```

Après la finalisation, les overrides TOML repassent par `exclude_absorbed_children` (`apply_overrides_complete`).

## 3. Invariants et pièges

- **La phase 3 ne voit que les tables qui ont un `TableEntry`.** `apply_wide_table_strategies` parcourt les chemins
  observés en Pass 1 et saute toute table qui n'est plus `Columns` (garde de #45 : un chemin peut désigner une table
  déjà fusionnée). Une table créée par la phase 2 n'a pas de `TableEntry` : elle ne reçoit jamais de stratégie large,
  quelle que soit sa largeur. Seule la garde des 1 600 colonnes la rattrape.
- **Une table de même nom qu'une autre est retirée.** Après la phase 1 et après la phase 2, un `retain` ne garde que
  la première table de chaque nom. Deux tables de même chemin sont la même table construite deux fois : la seconde
  disparaît sans message. Deux chemins sous un même nom sont une collision : la cascade retrouve ses tables par leur
  nom, les enfants des deux se mélangent et les routes vers la table retirée écrivent dans l'autre. `dedup_and_sort`
  l'annonce par un `WARNING` (et un `debug_assert` en test, #180). Sur OFF, avant #180, `nutriscore.<année>.data`
  écrivait ainsi dans la table du champ racine `nutriscore_data`. Les noms synthétiques se rendent donc uniques à leur
  création : `unique_cluster_suffix` (phase 2, et la passe keyed-pivot), `pick_unique_suffix` (`make_subgroup`),
  `unique_derived_table_name` (tables fusionnées de `cascade.rs`), `unique_pivot_name` (phase 3). Unique aussi
  contre les tables créées plus tôt dans la même vague (#136) : deux parents longs qui ne diffèrent que par les
  premiers mots que `derived_table_name` retire tirent le même nom de table pivot. La vague 0 choisit les noms de
  tous ses parents avant d'en créer une seule : `rename_shared_pivots` donne ensuite un hash du nom brut au doublon,
  sans changer son chemin. La passe keyed-pivot ajoute chaque sous-pivot dès sa création, pour que
  `unique_cluster_suffix` le voie au parent suivant (`…_key_2`). Avant #136, la seconde table était retirée : sur un
  test, 9 lignes perdues, et 27 lignes écrites dans la table de l'autre parent, dont la FK était refusée.
- **Les noms synthétiques passent par `derived_table_name`** (#180). `NamingRegistry` limite un nom de table à
  53 octets (`PG_TABLE_MAX_IDENT`), pour que `pk_…`, `fk_…_parent` et `j2s_…_id` tiennent dans les 63 de PostgreSQL.
  Un nom construit en ajoutant un suffixe (`{parent}_key`, `_num_2`, `_cluster_N`, `{table}_pivot`, table fusionnée
  `{parent}_{clé}`) est nettoyé, puis ses premiers mots sont retirés jusqu'à ce qu'il tienne : la fin, qui distingue
  les tables, reste lisible ; un hash seulement en dernier recours. Seul le nom change : `path`, `path_segment` et
  `absorbed_path_segments` restent bruts, la Pass 2 route avec. Le nettoyage confond des clés différentes (`es` /
  `ES`, `en:x` / `en-x`) : les fonctions de création comparent donc les noms dérivés, et une table fusionnée dont le
  nom est déjà pris par un autre chemin reçoit un hash de sa clé brute (`unique_derived_table_name`). Les décisions de
  la cascade (graine des regroupements, ordre du travail) suivent le chemin JSON, pas le nom : couper ou hasher un nom
  ne change pas les regroupements. `make_pivot_preamble` écrit encore la colonne parente à la main, sans l'assertion
  de `ColumnSchema::parent_fk`.
- **Rattacher une table à un autre parent passe par `reparent`** (#183). La cascade rattache sans fusion trois genres
  de table : le seul co-sibling d'un groupe (`handle_single_co_sibling`), le petit-enfant présent sous une seule des
  tables fusionnées (`cascade_grandchildren_to_next_wave`) et l'enfant unique d'un sous-pivot
  (`collect_pivot_co_siblings`). `reparent` change `parent_table` et refait la colonne parente avec
  `ColumnSchema::parent_fk`. Écrire `parent_table` à la main garde `j2s_{ancien parent}_id` : le DDL et la Pass 2
  trouvent la colonne par son indicateur `is_parent_fk`, donc tout marche, mais le nom en base désigne une table qui
  n'existe pas (sur OFF, `nutriscore_2023_data_components` avant #183). `test_every_table_name_fits_postgresql`
  vérifie ce nom sur toutes les fixtures. Seul `reparent_and_update_routes` écrit encore `parent_table` à la main :
  ses tables sont absorbées, puis retirées.
- **Un pivot `SiblingCollapseMulti` n'est pas une clé JSON** (#182). Pour fusionner les enfants de tables sœurs,
  `collect_children_by_key` les regroupe par le dernier segment de leur chemin. Le segment d'un pivot de groupe
  (`"key"`, `"num"`, `"num_2"`, `"cluster_N"`) n'est pas une clé du JSON : fusionné, il donnait une table routée
  sous une clé que la Pass 2 ne rencontre jamais, donc toujours vide (sur OFF, `…selected_key_sizes_key`, `_num` et
  `_num_2`).
  `is_multi_pivot` écarte ces pivots, d'après les `pivot_table` des groupes de la table sœur ; une autre table
  synthétique qui ne vient pas d'une clé du JSON devra être écartée au même endroit. Effet sur la vague 0 bis : la
  table fusionnée ne voit plus que de vraies clés. Sous `images.uploaded`, `full` était regroupée avec le faux `"num"`
  fusionné dans un pivot `_key` ; seule désormais, elle garde sa table `…_sizes_full`, et le pivot des tailles
  numériques s'appelle `_num` au lieu de `_num_2`. `test_every_child_route_key_is_a_json_key` vérifie la fixture
  `keyed_route_selected_images.jsonl`.
- **Une nouvelle forme de clé devient `"key"` en silence.** Le nom de la colonne clé est choisi par un `match` sur
  `KeyShape` avec un cas par défaut `_ => "key"`, recopié trois fois (deux dans `wave0.rs`, un dans
  `keyed_pivot.rs`).
- **`inferred_strategy` est écrit après la finalisation.** La doc du champ dit qu'il n'est plus modifié ;
  `apply_wide_strategy_columns` l'écrit pourtant pour `Pivot`, `Jsonb` et `StructuredPivot`, et la garde l'appelle.
  Les overrides qui l'appellent sauvegardent puis restaurent la valeur (voir la fiche des stratégies).
- **La garde retire les tables enfants.** `apply_column_limit_guard` dit garder les enfants de la table convertie ;
  mais il la passe en `Jsonb`, qui absorbe ses enfants, et `exclude_absorbed_children` les retire juste après. Un
  test de `registry.rs` vérifie ce retrait.
- **La phase 2 ne touche que les parents `Columns` ou `AutoSplit`** (`should_skip_parent`). Un parent déjà fusionné
  n'est pas refusionné : c'est ce qui rend les vagues successives sûres.
- **L'exclusion suppose les parents avant les enfants.** `exclude_absorbed_children` retire les descendants d'une
  table absorbante en un seul passage sur la liste. Le tri par profondeur puis chemin est refait après la phase 2, mais
  les compagnons de la phase 3 sont ajoutés à la fin sans nouveau tri : sans risque tant qu'ils n'ont pas d'enfants.
  Une table ajoutée en fin de liste avec des enfants casserait l'exclusion en cascade.
- **Une cible de `child_routes` survit, et ses propres cibles aussi** (#177). `collect_surviving_route_targets`
  protège les cibles des tables qui survivent, puis recommence avec les cibles protégées jusqu'à ce que rien ne
  change. `…_selected_key` est sous une table `SiblingCollapse` : sans ce point fixe, sa route `sizes` passait pour
  périmée, et `…_selected_key_sizes` était retirée alors que la Pass 2 y écrit.
- **Un chemin non pré-enregistré n'est vu qu'en debug.** `NamingRegistry::table_name_lookup_from_dot_key` a un
  `debug_assert` ; en release, il recalcule un nom qui peut différer de celui du registre (sans troncature à 53, sans
  suffixe d'unicité).
- **Les seuils arrivent par position.** `SchemaFinalizer::new` prend trois `f64` voisins (`sibling_jaccard`,
  `stable_threshold`, `rare_threshold`) : les intervertir compile (#112).

## 4. Points d'extension

Les endroits prévus sont listés au § 1. Trois changements demandent des étapes que le compilateur ne rappelle pas.

**Nouvelle forme de clé** (`KeyShape`).

- [ ] la variante, avec sa ligne dans le tableau du § 1 (le test `guides_consistency` le vérifie) ;
- [ ] sa détection dans `classify_key_shape` ;
- [ ] son libellé dans l'implémentation `Display` de `KeyShape` (le compilateur le signale) ;
- [ ] son nom de colonne clé dans les trois `match` à cas par défaut (§ 3) ;
- [ ] `schema-inference.md` § 7, tableau des formes de clé.

**Nouvelle table créée par la finalisation** (une table qui ne vient pas d'un chemin observé).

- [ ] un nom unique contre toutes les tables déjà connues (`unique_cluster_suffix`, `unique_pivot_name`), y compris
  celles créées plus tôt dans la même vague (§ 3), sinon le dédoublonnage la retire ;
- [ ] un nom passé par `derived_table_name` (53 octets au plus), et la colonne parente par `ColumnSchema::parent_fk` ;
- [ ] `parent_table`, `child_kind`, `depth` et `path` remplis : la Pass 2 retrouve la table par son chemin ;
- [ ] si la Pass 2 doit y router des sous-objets, une entrée dans `child_routes` du parent ;
- [ ] décider si elle doit recevoir une stratégie large : la phase 3 ne la verra pas (§ 3).

**Nouveau champ de `TableSchema`.** Le compilateur signale `TableSchema::new` et les constructions en littéral, pas
le reste :

- [ ] `#[serde(default)]` : le schéma est écrit dans le snapshot entre Pass 1 et Pass 2 ;
- [ ] les tables synthétiques de la phase 2 et de la phase 3 partent de `TableSchema::new` : vérifier la valeur
  qu'elles doivent porter ;
- [ ] ce que deviennent les tables fusionnées : le champ d'une table absorbée est perdu.

Couper une étape par `--disable-strategy` (`StrategyName::OPTIONAL`) relève de la fiche des stratégies (§ 4 C).

## 5. Vérifier

```bash
cargo test --lib schema::finalizer
cargo test --lib schema::cascading
cargo test --lib schema::registry
cargo test --lib schema::naming
cargo test --lib schema::wide_strategies
cargo test --lib schema::suffix_detector
cargo test --test guides_consistency
cargo test --lib --bins
TEST_DATABASE_URL=postgres://user:pass@localhost/testdb cargo test --test integration_strategies
cargo test --features integration-tests --test tc_sibling_collapse --test tc_wide_table
```

La plupart des tests de la finalisation sont dans `registry.rs` (#113), pas à côté du code : `schema::finalizer`
seul n'en lance qu'un. Les tests `tc_*`
démarrent un PostgreSQL dans Docker ; sans `TEST_DATABASE_URL`, `integration_strategies` affiche `[SKIP]` et passe.

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées. Déjà suivies ailleurs : réglages en sept arguments (#112), tests du
finaliseur dans `registry.rs` (#113), décisions en `matches!` et effet de bord sur `inferred_strategy` (odeurs 3 et 8
de la fiche des stratégies).

1. **Colonne clé choisie en trois copies** (#135). `KeyShape` → `key_id` / `lang_code` / `"key"` est écrit trois fois avec un
   cas par défaut. Une méthode de `KeyShape` sans cas par défaut ferait signaler une forme oubliée.
2. **Colonne parente écrite à la main** (#147). #136 a mesuré la phase 2 contre PostgreSQL : aucun nom coupé (les
   noms passent par `derived_table_name`, 53 octets au plus, depuis #180 ; `pk_name` / `fk_name` coupent à 63 avec un
   hash, #179), mais deux tables pivot qui prenaient le même nom, corrigées (§ 3). Reste la colonne parente :
   `make_pivot_preamble` et d'autres constructeurs de la cascade l'écrivent en littéral, sans l'assertion de
   `ColumnSchema::parent_fk`. Sans effet aujourd'hui : un parent fait 53 octets au plus, donc `j2s_…_id` au plus 60.
3. **La phase 3 parcourt les `TableEntry` pré-fusion** (#137) au lieu des schémas, d'où la garde de #45 et les tables
   synthétiques jamais élargies. `build_base_schemas` garde aussi un paramètre inutilisé.
4. **Docs décalées** (#138). `schema-inference.md` situe la finalisation dans `registry.rs` (`finalize_siblings`, aujourd'hui
   `finalize_cascading`), parle de stratégie « KeyedPivot » et d'une colonne `j2s_data` qui n'existe pas, et dit que
   l'exclusion des tables absorbées tourne dans `main.rs`. La doc d'`apply_column_limit_guard` (enfants gardés) et
   celle du champ `inferred_strategy` (jamais modifié) sont fausses (§ 3).
