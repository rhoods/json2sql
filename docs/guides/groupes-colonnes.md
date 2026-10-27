---
type: carte
sujet: Groupes de colonnes, proposition (--suggest-groups) et application (TOML, --group-columns, IHM)
verified_at: 5dfcced
files:
  - src/schema/grouping/
  - src/schema/column_groups.rs
  - src/schema/persistence.rs
  - src/schema/naming.rs
  - src/schema/config.rs
  - src/pipeline.rs
  - src/pass2/insert.rs
  - src/pass1/runner.rs
  - json2sql-ui/src/screens/common/overrides.rs
  - docs/functional/strategies.md
  - docs/functional/usage.md
  - docs/technical/schema-inference.md
  - docs/technical/modules.md
  - docs/guides/ajouter-une-strategie.md
  - docs/guides/pass1.md
  - tests/bench_column_groups.rs
  - tests/column_groups/mod.rs
  - tests/tc_column_groups.rs
symbols:
  - TableSchema
  - TableSchema::takes_column_groups
  - TableSchema::data_columns
  - apply_column_groups
  - dissolve_column_groups
  - prune_missing_groups
  - rebuilds_columns
  - reindex_stats
  - judge
  - check_group
  - forbid_emptying
  - MIN_GROUP_COLUMNS
  - apply_name_groups
  - name_proposals
  - confirmed_config
  - signals_of
  - suggest
  - suggest_groups
  - Suggestion
  - proposal_for
  - eligible
  - data_columns_by_name
  - key_path
  - data_signals
  - samples_of_rows
  - samples_of
  - RowSampler
  - MIN_TABLE_COLUMNS
  - propose
  - avoid_taken_names
  - TableProposal
  - Status
  - Evidence
  - Evidence::passes
  - Signal
  - DataSignals
  - DataSignals::verdict
  - DataSignals::evidence
  - evidence_text
  - LexicalParams
  - LexicalParams::default
  - lexical_groups
  - normalize
  - Presence
  - Shape
  - shape
  - Nature
  - ValueProfile
  - ValueProfile::observe_row
  - shared_value_pairs
  - rank_correlations
  - placements
  - StaticEmbedder
  - to_toml
  - to_markdown
  - sample_tables
  - observe_row
  - is_present
  - TableSample
  - StoredSample
  - DEFAULT_BUDGET
  - group_table_name
  - format_version
  - COLUMN_GROUPS_FORMAT_VERSION
  - SchemaSnapshot
  - SchemaConfig
  - post_process_schema
  - apply_schema_config
  - save_schema_snapshot
  - apply_group_columns
  - toml_column_group_tables
  - apply_overrides_complete
  - apply_user_overrides
  - build_effective_schemas
  - display_schemas
  - group_preview
  - splittable_tables
  - insert_object
  - write_group_rows
  - column_value
  - push_data_col
---

# Groupes de colonnes : proposer et appliquer

Carte de la zone `src/schema/grouping/` et `src/schema/column_groups.rs`, pour savoir où intervenir et ce qui casse
sans erreur de compilation. Le fonctionnement est décrit dans [strategies.md](../functional/strategies.md) (section
« Groupes de colonnes »), [usage.md](../functional/usage.md) (`column_groups`, `--group-columns`, `--suggest-groups`),
[schema-inference.md](../technical/schema-inference.md) et [modules.md](../technical/modules.md) ; le côté stratégies
dans [ajouter-une-strategie.md](ajouter-une-strategie.md) ; l'échantillon pris en Pass 1 (`RowSampler`, `samples_of`)
dans la [carte Pass 1](pass1.md).

## Rôle

Un groupe de colonnes est une **couche posée après la finalisation** : il découpe une table `Columns` ou `AutoSplit`
(`TableSchema::takes_column_groups`) en tables 1:1, une par thème de colonnes. La table garde sa stratégie. La zone
fait deux choses :

- **proposer** (`--suggest-groups`, #78) : un TOML `column_groups` et un rapport, sans toucher la base ;
- **appliquer** : `apply_column_groups` lit un TOML `column_groups` ; `apply_name_groups` applique les groupes
  confirmés que la zone propose elle-même (`--group-columns`, case « Column groups » de l'IHM, #91), en passant par
  le même `apply_column_groups`.

Une proposition part des **noms** : `normalize` réduit chaque nom à ses mots de thème, `lexical_groups` regroupe ces
radicaux. Un groupe d'une seule famille de préfixe est confirmé par les noms seuls ; un groupe qui **fusionne**
plusieurs familles n'est confirmé que si un **signal de données** (tableau ci-dessous) soutient assez de paires
(`Evidence::passes`), sinon il reste « à confirmer » et n'est jamais appliqué.

## 1. Où intervenir

| Changement | Où | Le compilateur aide ? |
|---|---|---|
| Nouveau signal de données | `Signal`, `DataSignals::verdict`, `DataSignals::evidence`, `evidence_text` (§ 4) | en partie : pas pour la liste de `DataSignals::evidence` |
| Seuils de la proposition | constantes en tête de `proposal.rs`, `LexicalParams::default`, `MIN_TABLE_COLUMNS` ; mesurer avec `tests/bench_column_groups.rs` (§ 4) | non |
| Découpe des noms | `normalize` (`names.rs`) | non |
| Sorties de `--suggest-groups` | `to_toml`, `to_markdown` (`render.rs`) | non |
| Règles d'un groupe appliqué | `apply_column_groups`, `judge`, `check_group`, `forbid_emptying`, `MIN_GROUP_COLUMNS` | non |
| Nom des tables de groupe | `group_table_name` (`naming.rs`), `avoid_taken_names` | non |
| Tables proposées | `eligible`, puis les filtres de `suggest` et de `name_proposals` (§ 3) | non |
| Échantillon | Pass 1 : `samples_of_rows` ; `--suggest-groups` : `sample_tables` ; stocké : `StoredSample` (§ 4) | non pour le snapshot |
| IHM | `build_effective_schemas`, `display_schemas`, `group_preview`, `splittable_tables` (`overrides.rs`) | non |
| Écriture en Pass 2 | `write_group_rows` (`insert.rs`) | non |
| Stratégie qui accepte ou non les groupes | `TableSchema::takes_column_groups`, voir [ajouter-une-strategie.md](ajouter-une-strategie.md) | non |

Les signaux de données, et ce qu'ils lisent dans l'échantillon d'une table :

| `Signal` | Lien entre deux colonnes | Colonnes jugées | Module |
|---|---|---|---|
| `Presence` | remplies ensemble : φ ajusté par richesse de la fiche, qui doit aussi battre nettement le bruit de fond de la table | présentes dans 2 % à 98 % des lignes (`Presence`) | `presence.rs` |
| `SharedValues` | mêmes valeurs, pondérées par leur rareté (`shared_value_pairs`) | texte (`Nature`), au moins cinq valeurs distinctes | `shared.rs` |
| `Rank` | corrélation de rang forte (`rank_correlations`) | numériques (`Nature`) | `numeric.rs` |
| `Shape` | même forme précise (`Shape` : année, epoch, degrés, via `shape`) | numériques avec une forme | `numeric.rs` |

Les embeddings (`StaticEmbedder`, `placements`, option `--embedding-model`) ne sont pas un signal : ils suggèrent dans
le rapport où iraient les colonnes restées dans la table, sans rien confirmer ni déplacer.

## 2. Flux

```text
CLI : post_process_schema (pipeline.rs)
  ├─ apply_schema_config → apply_overrides_complete → apply_column_groups (TOML), reindex_stats
  ├─ save_schema_snapshot                       ← le snapshot ne garde jamais de groupes automatiques
  └─ apply_group_columns (--group-columns, ou tables cochées dans le snapshot de l'IHM ;
     sauf tables dont le TOML a une clé column_groups : toml_column_group_tables)
       └─ apply_name_groups (auto.rs)
            ├─ name_proposals : tables eligible, pas déjà découpées, triées par nom
            │    └─ proposal_for(table, signals_of(table, échantillon Pass 1))
            │         └─ propose : normalize → lexical_groups → familles → DataSignals → statut
            │            puis avoid_taken_names
            └─ confirmed_config → apply_column_groups
       └─ reindex_stats

--suggest-groups : suggest_groups → suggest (suggest.rs)
  ├─ data_signals : relit le fichier (sample_tables, DEFAULT_BUDGET lignes à pas régulier)
  ├─ proposal_for pour chaque table eligible (+ placements avec un modèle)
  └─ Suggestion { toml: to_toml, report: to_markdown }

IHM : build_effective_schemas (overrides.rs)
  └─ apply_user_overrides, puis apply_name_groups sur les tables cochées

Pass 2 : insert_object → write_group_rows : une ligne par table de groupe, lue dans l'objet du parent
```

L'échantillon de la Pass 1 vient de `samples_of` (`runner.rs`), qui appelle `samples_of_rows` sur les lignes gardées
par `RowSampler` ; il est sauvé dans le snapshot (`SchemaSnapshot`, champ `samples`).

## 3. Invariants et pièges

- **Les colonnes d'une table sont prises triées par nom** (`data_columns_by_name`) : c'est l'ordre de l'échantillon
  (`StoredSample`, champ `columns`), des indices de `TableProposal` et des signaux. Une fonction qui parcourt
  `table.columns` à la place met les signaux d'une colonne sur une autre, **en silence**.
- **Les signaux s'éteignent sans avertissement en CLI et dans l'IHM.** `signals_of` (`auto.rs`) rend une raison au
  lieu de signaux quand la table n'a pas d'échantillon (vieux snapshot) ou que ses colonnes ont changé depuis la Pass 1
  (un `flatten` du TOML) ; la table est alors groupée par ses noms seuls, donc aucune fusion n'est appliquée. Cette
  raison n'est montrée nulle part (le rapport de `--suggest-groups` donne la sienne, celle de sa propre relecture).
- **Deux échantillons différents.** `--suggest-groups` relit le fichier (`sample_tables`, pas régulier sur les lignes
  racine, `DEFAULT_BUDGET`) ; `--group-columns` et l'IHM lisent l'échantillon de la Pass 1. Le TOML proposé et les
  groupes appliqués peuvent donc confirmer des fusions différentes sur le même fichier.
- **Lire une colonne comme la Pass 2.** `key_path` (`suggest.rs`) recopie la règle de `column_value` et
  `push_data_col` (`insert.rs`) : par `flatten_sources` si la colonne vient d'un enfant aplati, sinon par son nom
  d'origine. Changer l'une sans l'autre fait échantillonner une autre valeur que celle importée.
- **La liste des signaux est écrite en dur.** `DataSignals::evidence` parcourt un tableau fixe des variantes de
  `Signal`. Le compilateur signale `DataSignals::verdict` et `evidence_text` (des `match` sans cas par défaut), pas ce
  tableau : un signal oublié là compile, et n'est jamais calculé.
- **Ordre en CLI.** `apply_group_columns` vient après `save_schema_snapshot` : un snapshot ne contient que les groupes
  d'un TOML, jamais les automatiques. Une clé `column_groups` dans la section TOML d'une table, **même vide**, coupe les
  groupes automatiques pour cette table.
- **Les groupes automatiques sont recalculés à chaque chargement.** Changer un seuil, `normalize` ou un signal change
  en silence ce qu'importe un snapshot existant (cases cochées, `--group-columns`). Un TOML déjà produit par
  `--suggest-groups` reste, lui, figé.
- **Le même TOML doit donner le même schéma.** `SchemaConfig` range ses tables dans une `HashMap` ;
  `apply_column_groups` les trie par nom avant d'appliquer, et applique les groupes d'une table dans l'ordre de leurs
  noms. Une nouvelle boucle sur ces tables doit trier aussi.
- **Snapshot.** `format_version` n'écrit `COLUMN_GROUPS_FORMAT_VERSION` que si une table a des groupes : un programme
  plus ancien refuse alors le fichier. Les échantillons (`TableSample`, et dedans `Presence` et `ValueProfile`) sont
  sérialisés sans `#[serde(default)]` sur leurs champs : un champ ajouté rend illisible tout snapshot qui contient des
  échantillons, pas seulement ces échantillons.
- **Un groupe invalide est écarté, pas refusé.** `judge` et `check_group` (colonne inconnue, générée ou dans deux
  groupes, moins de `MIN_GROUP_COLUMNS` colonnes, nom déjà pris) et `forbid_emptying` (le groupe viderait la table)
  donnent un avertissement ; le reste du TOML s'applique.
- **Une stratégie qui reconstruit les colonnes défait les groupes** (`dissolve_column_groups`) : dans l'IHM,
  `apply_user_overrides` le fait pour `Pivot` et `Jsonb` ; dans le TOML, `rebuilds_columns` décide (stratégies `pivot`,
  `jsonb`, `normalize_dynamic_keys`, `flatten`, ou `suffix_columns`), deux listes à tenir ensemble. Après un `Skip`,
  `prune_missing_groups` retire les noms des tables de groupe disparues.
- **`reindex_stats` suit chaque application en CLI**, sinon les statistiques d'une colonne déplacée restent rangées
  sous l'ancienne table. L'IHM ne l'appelle pas : elle sauve les schémas d'origine, sans groupes.
- **`suggest` et `name_proposals` ne choisissent pas les mêmes tables, exprès.** `suggest` repropose une table déjà
  découpée par un TOML, sur les colonnes qui lui restent ; `name_proposals` l'écarte, pour qu'appliquer deux fois ne
  change rien. Les deux écartent les tables de groupe elles-mêmes.
- **Pass 2.** `write_group_rows` n'écrit une ligne de groupe que si l'une de ses colonnes a une valeur (`is_present` :
  ni `null`, ni `""`, ni `[]`, ni `{}`, la même règle que l'échantillon) ; sa ligne reprend l'identifiant de la ligne
  parente. Une table de groupe absente de `path_map` est sautée en silence.
- **Homonymes.** `signals_of` existe dans `auto.rs` et dans `suggest.rs` ; `observe_row` est une fonction de
  `sample.rs` et une méthode, `ValueProfile::observe_row`. Toujours dire de quel fichier on parle.

## 4. Points d'extension

Les endroits prévus sont listés au § 1. Trois changements demandent des étapes que le compilateur ne rappelle pas.

**Nouveau signal de données.**

- [ ] la variante dans `Signal`, et une ligne dans le tableau des signaux du § 1 (le test `guides_consistency` le
  vérifie) ;
- [ ] son calcul dans `DataSignals` et son cas dans `DataSignals::verdict` (le compilateur le signale), en rendant
  `None` pour une paire qu'il ne sait pas juger ;
- [ ] la variante dans le tableau de `DataSignals::evidence` (**le compilateur ne le voit pas**) ;
- [ ] son libellé dans `evidence_text` (le compilateur le signale) ;
- [ ] si le signal demande une donnée que l'échantillon ne garde pas : voir « Changer l'échantillon stocké » ;
- [ ] mesurer sur le banc, sans regarder les jeux `held_out` pour régler un seuil.

**Changer un seuil** (`LexicalParams::default`, constantes de `proposal.rs`, `MIN_TABLE_COLUMNS`) :

- [ ] lancer le banc `tests/bench_column_groups.rs` avant et après ; ses rapports de données et d'embeddings ne font
  rien sans `J2S_BENCH_DATA` et `J2S_BENCH_MODEL` (voir l'en-tête du fichier) ;
- [ ] les jeux `role = "held_out"` de `tests/fixtures/column_groups/` ne servent qu'à vérifier, jamais à régler ;
- [ ] un snapshot existant importera d'autres groupes (§ 3) : le dire dans la PR.

**Changer l'échantillon stocké** (`TableSample`, `Presence`, `ValueProfile`) :

- [ ] tout champ ajouté prend `#[serde(default)]`, sinon les snapshots existants deviennent illisibles ;
- [ ] les tableaux de nombres passent par `packed.rs` (forme hexadécimale), sinon le snapshot grossit d'un facteur dix ;
- [ ] remplir le champ à la fois dans `observe_row` (`sample.rs`), utilisé par la Pass 1 et par `sample_tables`.

## 5. Vérifier

```bash
cargo test --lib schema::grouping
cargo test --lib schema::column_groups
cargo test --lib pipeline
cargo test --test bench_column_groups
cargo test --test guides_consistency
cargo test --lib --bins
cargo check --workspace --all-targets
cargo test --features integration-tests --test tc_column_groups
```

`tc_column_groups` démarre un PostgreSQL dans Docker (testcontainers) : c'est le seul test qui écrit vraiment les
tables de groupe. Les autres ne demandent pas de base.

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées.

1. **Deux échantillonneurs** (#152). `--suggest-groups` relit le fichier avec sa propre règle (`sample_tables`), alors que
   `--group-columns` et l'IHM utilisent l'échantillon de la Pass 1 : deux codes, et un TOML proposé qui peut confirmer
   d'autres fusions que celles appliquées.
2. **La lecture d'une colonne est écrite deux fois** (#153) : `key_path` (`suggest.rs`) et `column_value` (`insert.rs`).
3. **La liste des signaux est écrite en dur** (#154) dans `DataSignals::evidence` ; une constante de toutes les variantes,
   vérifiée par un `match` sans cas par défaut, rendrait l'oubli visible au compilateur.
4. **La raison des signaux éteints se perd** (#155) en CLI et dans l'IHM (`signals_of` de `auto.rs`) : un `flatten` du TOML
   retire toutes les fusions sans que l'utilisateur le sache.
