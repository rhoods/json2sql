---
type: carte
sujet: Base de données, création des tables, contraintes, réglages du serveur et connexions
verified_at: 527afa3
files:
  - src/db/connection.rs
  - src/db/ddl.rs
  - src/db/pg_tuning.rs
  - src/db/error.rs
  - src/pass2/runner/mod.rs
  - src/pipeline.rs
  - json2sql-worker/src/pipeline.rs
  - json2sql-ui/src/screens/preview.rs
  - json2sql-ui/src/screens/setup.rs
  - src/schema/type_tracker.rs
  - src/schema/naming.rs
  - src/schema/config.rs
  - docs/technical/architecture.md
  - docs/technical/modules.md
  - tests/tc_pg_tuning.rs
  - tests/tc_nested_fk.rs
symbols:
  - connect
  - cancel_on_drop
  - pg_err
  - connect_and_create_tables
  - print_dry_run_ddl
  - create_tables_no_constraints
  - build_col_defs
  - generate_create_table
  - generate_create_table_no_constraints
  - generate_add_pk_sql
  - generate_add_fk_sql
  - generate_validate_fk_sql
  - generate_ddl_preview
  - generate_autovacuum_sql
  - generate_vacuum_analyze_sql
  - quote_ident
  - add_constraints
  - add_pk
  - validate_fk
  - drop_unvalidated_fk
  - drop_parent_fks
  - existing_pks
  - to_fk_warning
  - open_constraints_conn
  - try_configure_constraints_conn
  - try_set_synchronous_commit_off
  - pk_mem_mb
  - calc_maintenance_work_mem_mb
  - table_sizes
  - largest_first
  - set_tables_autovacuum
  - vacuum_analyze_tables
  - ConstraintWarning
  - ConstraintKind
  - run_interruptible
  - load_and_constrain
  - put_back_autovacuum
  - preflight_warn_nonempty
  - restore_pending
  - warn_pg_tuning
  - warn_wal_level
  - tuning_hints
  - wal_level_hint
  - raise_max_wal_size
  - record_then_raise
  - restore_max_wal_size
  - record_autovacuum_restore
  - forget_restore
  - set_marker
  - marker_path
  - WalRestore
  - TUNED_MAX_WAL_SIZE_MB
  - PgType
  - PgType::as_sql
  - parse_pg_type
  - coerce
  - PG_TABLE_MAX_IDENT
  - TableSchema
---

# Base de données : tables, contraintes, réglages

Carte de la couche PostgreSQL (`src/db/`), pour savoir où intervenir et ce qui casse sans erreur de compilation. Le
fonctionnement est décrit dans [architecture.md § src/db/](../technical/architecture.md#srcdb) et, pour les réglages
du serveur et l'annulation, dans [architecture.md § src/pass2/](../technical/architecture.md#srcpass2). L'écriture des
lignes (`COPY`, `copy_sink.rs`, `copy_text.rs`) a sa fiche : [pass2.md](pass2.md).

## Rôle

Avant la Pass 2, créer les tables (colonnes seules, sans clés). Après le chargement, les finir : `VACUUM (ANALYZE)`,
clés primaires (PK), puis clés étrangères vers le parent (FK). Pendant tout l'import, régler le serveur pour un gros
chargement (autovacuum coupé, `max_wal_size` relevé sur demande), et **toujours** le remettre comme avant, même après
un `kill -9`.

## 1. Où intervenir

| Changement | Où | Le compilateur aide ? |
|---|---|---|
| Nouveau type PostgreSQL | `PgType` et `PgType::as_sql` (rendu SQL, tableau ci-dessous) ; côté Pass 1, la checklist de [pass1.md](pass1.md) ; côté écriture, `coerce` ([pass2.md](pass2.md)) | oui pour `PgType::as_sql` et `coerce` ; **non** pour `parse_pg_type` (types des overrides TOML) |
| Définition d'une colonne (type, `NOT NULL`) | `build_col_defs`, commun à tous les `CREATE TABLE` | non |
| Nom d'une contrainte, nouvelle contrainte | écrit en dur dans cinq fonctions (§ 4) | **non** |
| Ordre ou parallélisme de la phase des contraintes, mémoire des PK | `add_constraints`, `pk_mem_mb`, `calc_maintenance_work_mem_mb` ; lire d'abord architecture.md | non |
| Réglage de session des connexions de contraintes | `try_configure_constraints_conn` (le `VACUUM` règle sa propre connexion dans `vacuum_analyze_tables`, le chargement les siennes, voir pass2.md ; `try_set_synchronous_commit_off` sert aux deux) | non |
| Réglage temporaire du serveur (comme `max_wal_size`) | `pg_tuning.rs`, sur le modèle de `record_then_raise` (§ 4) | non |
| Conseil au démarrage (réglage serveur peu adapté) | `tuning_hints` ou `wal_level_hint`, fonctions pures testées sans base | non |
| Connexion (TLS, délai d'attente) | quatre endroits ouvrent une connexion (§ 3) | non |

Rendu SQL de chaque type et type accepté dans un override TOML (`parse_pg_type`). Le test `guides_consistency`
vérifie que chaque variante de `PgType` a sa ligne.

| Variante | Rendu DDL | Override TOML |
|---|---|---|
| `Integer` | `INTEGER` | oui (`INT`, `INT4`) |
| `BigInt` | `BIGINT` | oui (`INT8`) |
| `DoublePrecision` | `DOUBLE PRECISION` | oui (`FLOAT`, `FLOAT8`, `REAL`, `FLOAT4`) |
| `Boolean` | `BOOLEAN` | oui (`BOOL`) |
| `Uuid` | `UUID` | oui |
| `Date` | `DATE` | oui |
| `Timestamp` | `TIMESTAMP` | oui (`TIMESTAMP WITHOUT TIME ZONE`) |
| `VarChar` | `VARCHAR(n)` | oui (`CHARACTER VARYING(n)`) |
| `Text` | `TEXT` | oui |
| `Array` | type de l'élément suivi de `[]` | non : un override sur une colonne tableau est ignoré, avec un avertissement |
| `Jsonb` | `JSONB` | non (type inconnu, avertissement) |

## 2. Flux

```text
CLI : connect_and_create_tables ─┐
IHM : run_pipeline (worker)     ─┴─► create_tables_no_constraints
                                       DROP … CASCADE de toutes les tables (si --drop-existing), un seul envoi
                                       CREATE TABLE IF NOT EXISTS par lots de 50, sans PK ni FK
run_interruptible
  ├─ restore_pending       : retours arrière laissés par un import interrompu
  ├─ warn_pg_tuning        : conseils shared_buffers / max_wal_size
  ├─ raise_max_wal_size    : si --tune-wal (marqueur écrit avant le ALTER SYSTEM)
  ├─ record_autovacuum_restore, puis set_tables_autovacuum(false)
  ├─ load_and_constrain
  │    ├─ drop_parent_fks      : FK enfant → parent d'un import précédent retirées (sauf --no-constraints)
  │    ├─ chargement COPY (pass2.md)
  │    ├─ vacuum_analyze_tables : la plus grosse table d'abord, sans freinage
  │    └─ add_constraints (sauf --no-constraints)
  │         1. PK manquantes (existing_pks), plus grosses tables d'abord, au plus 4 connexions, mémoire prise dans un budget commun
  │         2. FK ajoutées NOT VALID, une connexion, instantané
  │         3. VALIDATE de chaque FK, en parallèle
  ├─ put_back_autovacuum   : toujours, même en erreur
  └─ restore_max_wal_size  : toujours, même en erreur
```

Les deux points d'entrée (CLI et worker de l'IHM) créent les tables chacun de leur côté, puis passent tous deux par
`run_interruptible` : tout ce qui suit la création est commun.

## 3. Invariants et pièges

- **Le DDL affiché n'est pas le DDL exécuté.** Trois rendus du `CREATE TABLE` existent :
  `generate_create_table_no_constraints` (exécuté), `generate_create_table` (le `--dry-run`, via
  `print_dry_run_ddl`, avec la PK dans le `CREATE` et sans `fillfactor = 100`, et une FK sans `NOT VALID` recopiée
  dans `pipeline.rs`), et `generate_ddl_preview` (l'aperçu de l'IHM, PK et FK dans le `CREATE`). Une modification
  du DDL exécuté ne se voit ni dans le dry-run ni dans l'aperçu tant qu'on ne les modifie pas aussi.
- **Relancer un import sans `--drop-existing` ajoute les lignes et garde les PK.** Les tables existantes sont
  gardées (`IF NOT EXISTS`). Avant le chargement, `drop_parent_fks` retire les FK enfant → parent : laissées en
  place, elles refuseraient un enfant écrit avant son parent (23503), et le chargement repasse en une transaction
  par connexion. Après le chargement, `add_constraints` saute les tables qui ont déjà une PK (`existing_pks` : la
  reconstruire trierait toute la table), puis repose les FK : le `VALIDATE` relit toute la table enfant, anciennes
  lignes comprises. Si le chargement échoue entre les deux, les tables restent sans FK jusqu'au prochain import.
  Avec `--no-constraints`, rien n'est retiré : des FK déjà posées peuvent alors refuser le chargement. PK et FK
  sont trouvées par type dans `pg_constraint`, pas par nom. (#146)
- **Une table existante garde ses anciennes colonnes.** Avec `IF NOT EXISTS`, un schéma qui a changé n'est pas
  appliqué : le `COPY` liste ses colonnes, donc une colonne absente le fait échouer, une colonne en trop reste `NULL`.
- **`DROP … CASCADE` emporte aussi les objets de l'utilisateur** qui dépendent des tables (vues, FK d'autres tables).
- **Noms de contraintes à la limite.** Les contraintes s'appellent `pk_<table>` et `fk_<table>_parent`. Un nom de
  table fait au plus `PG_TABLE_MAX_IDENT` (53) caractères : `fk_` + 53 + `_parent` = 63, exactement la limite de
  PostgreSQL. Avec un préfixe ou un suffixe plus long, PostgreSQL tronque le nom (un simple `NOTICE`, que
  json2sql n'affiche pas) et deux contraintes peuvent alors porter le même nom.
- **`NOT NULL` seulement sur les colonnes générées.** `build_col_defs` ne l'écrit que si la colonne est `not_null`
  **et** générée (`j2s_…`). Une colonne de données est toujours nullable : une valeur non convertie devient `NULL`
  (voir pass2.md).
- **Une PK qui échoue arrête l'import, une FK qui échoue non.** Une erreur de PK est fatale (doublon de `j2s_id` = bug).
  Une FK qui ne se valide pas devient un `ConstraintWarning` et la contrainte est supprimée : la table reste sans FK.
- **Le compteur de progression des contraintes est calculé à part.** `add_constraints` compte une étape par table
  (PK) plus une par table enfant (FK). Une nouvelle contrainte doit aussi entrer dans ce total, sinon la barre de
  progression de l'IHM finit avant ou après 100 %, sans erreur.
- **Certains avertissements ne vont qu'à stderr.** Ceux de `try_configure_constraints_conn` et les durées de `add_pk`
  passent par `eprintln!` : l'IHM ne les voit pas. Ceux de `pg_tuning.rs` passent aussi par l'IHM.
- **Le marqueur d'abord, le serveur ensuite.** Chaque retour arrière est écrit dans le fichier marqueur
  (`marker_path`, sous verrou dans `set_marker`) **avant** de modifier le serveur, et chaque Pass 2 commence par
  `restore_pending`. Inverser l'ordre laisse le serveur modifié après un `kill -9`.
- **Le retour de l'autovacuum efface un réglage de l'utilisateur.** `generate_autovacuum_sql` remet l'autovacuum par
  `RESET (autovacuum_enabled)`, pas à sa valeur d'avant (limite connue, commentée dans le code).
- **Les requêtes longues passent par `cancel_on_drop`.** Une requête abandonnée sans lui continue côté serveur et
  garde ses verrous. Toute nouvelle requête longue (index, `VACUUM`, `VALIDATE`) doit y passer.
- **Quatre façons d'ouvrir une connexion.** `connect` (CLI, chargement, `VACUUM`, autovacuum), `open_constraints_conn`
  (contraintes, la connexion réglée), le worker (`tokio_postgres::connect` avec 10 s de délai) et le bouton de test
  de l'écran de configuration de l'IHM (5 s). Ajouter TLS ou un délai se fait dans les quatre.
- **`generate_ddl_preview` porte `#[allow(dead_code)]`** mais l'IHM l'appelle : ne pas le supprimer.
- **Les tests PostgreSQL passent au vert sans base.** Sans `TEST_DATABASE_URL`, ils affichent `[SKIP]`. Les tests
  `tc_*` démarrent un PostgreSQL dans Docker.

## 4. Points d'extension

Les fonctions `generate_*` de `ddl.rs` sont pures (du texte SQL, sans base) : chaque changement de DDL commence par
un test unitaire dans `ddl.rs`, comme ceux qui existent.

**Renommer une contrainte, ou en ajouter une.** Les noms `pk_<table>` et `fk_<table>_parent`, et la recherche de
la colonne parente (repli sur `j2s_parent_id`), sont écrits en dur. À faire à la main :

- [ ] `generate_add_pk_sql`, `generate_add_fk_sql`, `generate_validate_fk_sql` ;
- [ ] la suppression de la FK non validée dans `drop_unvalidated_fk` ;
- [ ] l'aperçu `generate_ddl_preview`, le dry-run `generate_create_table` et `print_dry_run_ddl` ;
- [ ] pour une nouvelle contrainte : sa phase dans `add_constraints`, son compte dans le total de progression,
  fatale ou en `ConstraintWarning` ;
- [ ] vérifier la longueur du nom (≤ 63 avec une table de 53 caractères) ;
- [ ] un test PostgreSQL (`tc_nested_fk.rs` pose les PK et FK sur un vrai serveur).

**Nouveau réglage temporaire du serveur.** Suivre `raise_max_wal_size` :

- [ ] lire la valeur actuelle, et ne rien faire si elle convient déjà ;
- [ ] écrire le retour arrière dans le marqueur **avant** de changer le serveur (`record_then_raise`), sous une clé
  qui identifie le serveur ou la base, sans mot de passe ;
- [ ] l'appliquer au démarrage dans `restore_pending`, et à la fin dans `run_interruptible`, quel que soit le
  résultat ; puis retirer l'entrée du marqueur (`forget_restore`) ;
- [ ] un manque de droits donne un avertissement, jamais une erreur ;
- [ ] un test dans `tc_pg_tuning.rs`, y compris l'import interrompu.

**Nouveau type PostgreSQL**, côté base : son rendu dans `PgType::as_sql` (le compilateur le signale), une ligne dans
le tableau du § 1 (le test le vérifie), et `parse_pg_type` s'il doit être accepté dans un override TOML (le
compilateur ne le signale pas).

## 5. Vérifier

```bash
cargo test --lib db::
cargo test --test guides_consistency
cargo test --features integration-tests --test tc_pg_tuning --test tc_nested_fk
TEST_DATABASE_URL=postgres://user:pass@localhost/testdb cargo test --test integration_schema
```

Les tests `tc_*` démarrent un PostgreSQL dans Docker (testcontainers) : il faut Docker, pas `TEST_DATABASE_URL`.

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées ; chacune a son issue.

1. **Noms de contraintes recopiés.** `pk_<table>`, `fk_<table>_parent` et la recherche de la colonne parente sont
   écrits dans cinq fonctions, dont une dans `pipeline.rs`. Une fonction par nom les garderait d'accord. (#147)
2. **Trois rendus du `CREATE TABLE`**, déjà différents de ce qui est exécuté (§ 3). Le dry-run et l'aperçu
   pourraient afficher la suite exacte des requêtes exécutées. (#148)
3. **Quatre façons d'ouvrir une connexion**, avec des délais différents ; `to_fk_warning` recopie la mise en forme
   de `pg_err`. (#149)
4. **Une variante jamais utilisée.** `ConstraintKind` a une variante `PrimaryKey` jamais construite, et le champ qui
   la porte n'est jamais lu (tous deux marqués `#[allow(dead_code)]`). (#150)
5. **Docs décalées.** architecture.md dit que l'aperçu sert au dry-run (c'est `generate_create_table`), que
   l'autovacuum revient « avec un `ANALYZE` avant les contraintes » (il revient après, le `VACUUM (ANALYZE)` est
   lancé à part) et que `PrimaryKey` est « toujours fatal » (la variante n'est jamais créée) ; modules.md parle
   d'« ordre topologique » et n'a pas de ligne pour `pg_tuning.rs`. (#151)
