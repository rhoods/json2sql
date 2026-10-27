---
type: carte
sujet: CLI et pipeline d'import, des options de la ligne de commande aux rapports
verified_at: 98969c6
files:
  - src/cli.rs
  - src/main.rs
  - src/pipeline.rs
  - src/error.rs
  - src/anomaly/reporter.rs
  - src/ipc.rs
  - src/schema/strategies.rs
  - json2sql-worker/src/pipeline.rs
  - json2sql-ui/src/screens/analysis.rs
  - json2sql-ui/src/screens/common/overrides.rs
  - docs/functional/usage.md
  - docs/technical/architecture.md
  - tests/tc_column_groups.rs
symbols:
  - Cli
  - Commands
  - Cli::root_table_name
  - main
  - run_inspect
  - build_inspect_registry
  - run_suggest_groups
  - PipelineConfig
  - run_pipeline
  - suggest_groups
  - finalized_schema
  - resolve_input
  - load_or_infer_schema
  - restore_from_snapshot
  - run_pass1_workers
  - report_pass1_warnings
  - post_process_schema
  - apply_schema_config
  - save_schema_snapshot
  - apply_group_columns
  - emit_schema_report
  - print_dry_run_ddl
  - connect_and_create_tables
  - run_pass2
  - ctrl_c_interrupt
  - finalize_pass2
  - report_pass2_results
  - write_report
  - AnomalyFormat
  - J2sError
  - write_sample_file
  - run_interruptible
  - Pass1Config
  - Pass2Config
  - RegistryConfig
  - WorkerConfig
  - WorkerConfig::into_pass2_config
  - parse_disabled_strategies
  - StrategyName
  - StrategyName::OPTIONAL
---

# CLI et pipeline d'import

Carte de la ligne de commande (`src/cli.rs`, `src/main.rs`) et de l'enchaînement d'un import (`src/pipeline.rs`), pour
savoir où intervenir et ce qui casse sans erreur de compilation. Les options sont décrites dans
[usage.md](../functional/usage.md), la séquence d'un import dans
[architecture.md § src/main.rs et src/pipeline.rs](../technical/architecture.md#srcmainrs-et-srcpipeliners). La Pass 1
et la Pass 2 ont leurs propres fiches : [pass1.md](pass1.md), [pass2.md](pass2.md).

## Rôle

`Cli` (clap) lit les options ; `main` les recopie dans un `PipelineConfig`, qui ne connaît plus clap ;
`run_pipeline` (celui de `src/pipeline.rs`) enchaîne la Pass 1 ou un snapshot, les surcharges, le DDL, la Pass 2 et
les rapports. La zone décide **de l'ordre des étapes et de ce qui sort où** ; elle n'infère ni n'insère rien elle-même.

## 1. Où intervenir

| Changement | Où | Le compilateur aide ? |
|---|---|---|
| Nouvelle option d'import | un champ de `Cli`, sa recopie dans `main`, un champ de `PipelineConfig`, l'étape qui le lit | **non** pour la recopie dans `main` (§ 3) ; oui ensuite, `PipelineConfig` est construit en littéral partout |
| Nouvelle option de la Pass 1 | en plus : `RegistryConfig` dans `run_pass1_workers`, puis la fiche [pass1.md](pass1.md) | oui pour `RegistryConfig` (littéral) |
| Nouvelle option de la Pass 2 | en plus : `Pass2Config` dans `run_pass2`, et côté IHM `WorkerConfig::into_pass2_config` (voir [pass2.md](pass2.md)) | oui pour `Pass2Config` (littéral) |
| Nouvelle étape sur le schéma (après la Pass 1, avant le DDL) | `post_process_schema`, à sa place dans l'ordre (§ 3) | non |
| Nouveau mode qui remplace l'import (comme `--suggest-groups`) | `main` choisit entre `run_suggest_groups` et `run_pipeline` ; la fonction du mode réutilise `finalized_schema` | non |
| Nouvelle sous-commande | une variante de `Commands` et son cas dans le `match` de `main` (sans cas par défaut) | oui |
| Nouveau format de rapport d'anomalies | une variante d'`AnomalyFormat` et son cas dans `write_report` (sans cas par défaut) ; la valeur CLI vient du nom de la variante | oui |
| Nouvelle erreur | une variante de `J2sError` dans `src/error.rs` | non : `main` affiche toute erreur de la même façon |
| Nouvel avertissement de la Pass 1 | une fonction `warn_*` appelée par `report_pass1_warnings` | non |

## 2. Flux

```text
main (src/main.rs)
├─ Commands::Inspect → run_inspect (main.rs) → pass1::runner::run_inspect : Pass 1 sur N objets, sans stratégie
└─ sans sous-commande : parse_disabled_strategies, Cli → PipelineConfig
   ├─ --suggest-groups → run_suggest_groups → suggest_groups → finalized_schema → proposition (aucune base)
   └─ sinon → run_pipeline (src/pipeline.rs)
        ├─ finalized_schema
        │   ├─ resolve_input : stdin copié dans un fichier temporaire (le fichier est lu deux fois)
        │   ├─ load_or_infer_schema : restore_from_snapshot (--schema-input) ou run_pass1_workers
        │   ├─ report_pass1_warnings
        │   └─ post_process_schema : apply_schema_config → save_schema_snapshot → apply_group_columns
        │                            → emit_schema_report
        ├─ --dry-run → print_dry_run_ddl, fin
        ├─ connect_and_create_tables : DDL sans contraintes
        ├─ run_pass2 → run_interruptible (Ctrl-C via ctrl_c_interrupt)
        └─ finalize_pass2 : report_pass2_results, write_report, contrôle de --max-anomaly-rate
```

L'IHM n'emprunte pas ce chemin : elle lance la Pass 1 elle-même (`json2sql-ui/src/screens/analysis.rs`), applique ses
surcharges (`overrides.rs`), puis envoie les schémas finis au worker dans un `WorkerConfig` ; le `run_pipeline` du
worker (`json2sql-worker/src/pipeline.rs`, un autre que celui de `src/pipeline.rs`) crée les tables et appelle
`run_interruptible`.

## 3. Invariants et pièges

- **Une option de `Cli` non recopiée dans `main` est acceptée et sans effet, en silence.** Les champs de `Cli` sont
  publics : le compilateur ne signale pas un champ jamais lu. C'est le cas aujourd'hui de `--batch-size` et
  `--transaction`, parsés puis ignorés (usage.md les marque « sans effet »). La suite, elle, est protégée :
  `PipelineConfig` est construit en littéral dans `main`, dans les tests de `pipeline.rs` et dans les `tests/tc_*.rs`,
  qui cassent tous à l'ajout d'un champ.
- **Deux chaînes, CLI et IHM.** Une option ajoutée à la CLI n'existe pas dans l'IHM : ni `PipelineConfig`, ni
  `post_process_schema`, ni les rapports de `finalize_pass2` n'y servent. Les défauts des seuils diffèrent déjà entre
  les deux (#112). L'IHM a sa carte (#100).
- **`--schema-input` ignore en silence toutes les options de la Pass 1** (`--text-threshold`,
  `--wide-column-threshold`, `--workers`, `--disable-strategy`…) : `load_or_infer_schema` ne lance pas
  `run_pass1_workers`. Le fichier d'entrée reste nécessaire pour la Pass 2.
- **stdout porte les résultats, stderr la progression.** Sortent sur stdout : le DDL de `--dry-run`, le rapport
  d'anomalies sans `--anomaly-output`, le rapport de `--suggest-groups` et le schéma d'`inspect` sans `--output`.
  Un message de progression écrit avec `println!` se mélange à ces sorties redirigées ; il va sur stderr
  (`eprintln!`).
- **L'ordre de `post_process_schema` est voulu.** Surcharges du TOML, puis snapshot, puis groupes de colonnes, puis
  rapport : le snapshot est écrit avant les groupes automatiques pour qu'un TOML donné plus tard l'emporte encore
  (test `the_snapshot_is_saved_before_the_strategy` de `pipeline.rs`). Une étape insérée au mauvais endroit change
  ce que contient le snapshot, sans erreur.
- **`--max-anomaly-rate` agit après le COMMIT.** `finalize_pass2` compare le taux une fois la Pass 2 finie : les
  lignes sont déjà en base, seuls le message et le code de sortie changent. usage.md dit « abort ».
- **Le rapport d'anomalies n'est écrit que s'il y a des anomalies, ou si `--anomaly-output` est donné.** Sans
  anomalie et sans fichier, stdout reste vide.
- **Le fichier temporaire de stdin vit tant que sa garde vit.** `finalized_schema` rend la garde en premier élément ;
  la lier au seul tiret bas (au lieu de `_stdin_temp`) supprime le fichier aussitôt, et la Pass 2 ne trouve plus
  son entrée.
- **`--limit` a trois sens.** Avec `inspect` : objets lus par la Pass 1 (défaut 500). À l'import : objets racines
  envoyés à la Pass 2 (la Pass 1 lit tout). Avec `--suggest-groups` : lignes racines relues pour la proposition.
- **`inspect` n'utilise que `--text-threshold`.** Le `RegistryConfig` construit par `run_inspect` (main.rs) est
  remplacé par `build_inspect_registry` (#112) : y ajouter un réglage n'a aucun effet.
- **Toute erreur sort de la même façon.** `main` convertit chaque `J2sError` en message et code de sortie 1 ; une
  erreur d'option de clap sort avec le code 2. Une variante de `J2sError` ne change donc pas le code de sortie.

## 4. Points d'extension

Les endroits prévus sont au § 1. Trois demandent des étapes que le compilateur ne rappelle pas.

**Nouvelle option d'import.** À faire à la main :

- [ ] le champ dans `Cli`, avec son aide (`///`) et son défaut ; les exclusions entre options en `conflicts_with` /
  `requires` ;
- [ ] sa recopie dans le littéral `PipelineConfig` de `main` (rien ne la force, § 3) ;
- [ ] l'étape qui la lit, et `RegistryConfig` ou `Pass2Config` si elle descend dans une passe ;
- [ ] une ligne `` | `--option` | défaut | description | `` dans le bon tableau de usage.md (le test
  `cli_options_documented` le vérifie) ;
- [ ] un test `Cli::try_parse_from` dans `cli.rs` (défaut et valeur) ;
- [ ] décider si l'IHM doit l'avoir : `WorkerConfig` et la fiche IHM.

**Nouveau mode à la place de l'import.** Prendre `--suggest-groups` pour modèle : une option `Option<PathBuf>`
exclusive des modes voisins (`conflicts_with`), choisie dans `main`, une fonction publique dans `pipeline.rs` qui
passe par `finalized_schema` (mêmes Pass 1, snapshot et surcharges que l'import) et ne touche pas la base. Écrire le
résultat sur stdout ou dans le fichier, la progression sur stderr.

**Nouvelle valeur pour `--disable-strategy`.** Les valeurs viennent de `StrategyName::OPTIONAL` et sont lues par
`parse_disabled_strategies`, mais l'aide de l'option dans `cli.rs` et la ligne de usage.md les recopient à la main :
les deux sont vérifiés par `tests/guides_consistency.rs`. Le reste est dans
[ajouter-une-strategie.md](ajouter-une-strategie.md).

## 5. Vérifier

```bash
cargo test --lib cli
cargo test --lib pipeline
cargo test --test guides_consistency
cargo test --lib --bins
cargo check --workspace --all-targets
cargo test --features integration-tests --test tc_column_groups --test tc_wide_table --test tc_sibling_collapse
```

Les tests `tc_*` appellent `run_pipeline` de bout en bout sur un PostgreSQL lancé dans Docker (testcontainers) : il
faut Docker. Pour voir les sorties, `cargo run -- --input examples/products.jsonl --dry-run` (DDL sur stdout) et
`cargo run -- inspect examples/products.jsonl`.

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées.

1. **`--batch-size` et `--transaction` acceptés sans effet.** Un script qui les passe croit obtenir un comportement
   qu'il n'a pas. Les retirer (erreur explicite) ou les câbler.
2. **`--max-anomaly-rate` annoncé comme un abandon** alors qu'il agit après le COMMIT (§ 3).
3. **`J2sError::Json` annonce une position « at byte »** mais reçoit la colonne de serde_json, ou `0` dans
   `write_sample_file`.
4. **Le nom de la table racine calculé deux fois** : `Cli::root_table_name` pour l'import, une copie dans le cas
   `Commands::Inspect` de `main`.
5. **Docs décalées** : l'en-tête de `reporter.rs` cite `--anomaly-report` (l'option est `--anomaly-output`) ;
   architecture.md dit que le worker de l'IHM appelle le pipeline, alors qu'il a son propre `run_pipeline`.

Déjà suivi ailleurs : les défauts différents entre CLI et IHM, et le `RegistryConfig` inutile d'`inspect` (#112).
