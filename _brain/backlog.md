# Backlog — json2sql

> Généré le 2026-06-19 — revue collective brain-party-mode.
> À importer dans GitHub Issues quand `gh` sera disponible.

---

## 🐛 BUG

### #1 — Retirer les `println!` de debug dans `finalizer.rs::apply_wide_strategy()`

**Priorité : immédiate**

**Problème**
6 `println!` bruts ont été laissés dans `apply_wide_strategy()` (`src/schema/finalizer.rs`, lignes 312–343). Ils s'exécutent pour chaque table du schéma et polluent stdout pendant tout run Pass 1. Sur un fichier Open Food Facts (500+ tables), ça génère des centaines de lignes de bruit mélangées à la progress bar.

**Ce qui doit être fait**
- Supprimer les 6 `println!` (lignes 312, 313, 314, 331, 341, 343)
- Conserver les `eprintln!` intentionnels (lignes 334–340, 365–377, 429–434)

**Fichiers impactés**
- `src/schema/finalizer.rs`

---

## ⚠️ DEBT technique

### #2 — Introduire des config structs pour les fonctions à trop de paramètres (T5)

**Problème**
Trois fonctions contournent Clippy `too_many_arguments` avec `#[allow]`, étiquetées `// T5: candidate for RegistryConfig struct` :
1. `SchemaRegistry::new()` — 8 paramètres (`src/schema/registry.rs:36`)
2. `AnomalyCollector` — `src/anomaly/collector.rs:267`
3. Fonction dans `src/pass2/runner.rs:446`

**Ce qui doit être fait**
- Introduire un config struct par site (ex. `RegistryConfig`) sur le modèle de `Pass1Config`
- Migrer les call sites dans `pass1/runner.rs`, `pipeline.rs`, `main.rs`, et l'IHM Dioxus
- Retirer les `#[allow(clippy::too_many_arguments)]` correspondants

**Fichiers impactés**
- `src/schema/registry.rs`, `src/anomaly/collector.rs`, `src/pass2/runner.rs`
- `src/pass1/runner.rs`, `src/pipeline.rs`, `src/main.rs`, `json2sql-ui/`
- Tests qui construisent directement `SchemaRegistry::new(...)`

---

### #3 — Séparer `InferredStrategy` en deux enums distincts (inféré vs. override manuel)

**Problème**
`InferredStrategy` dans `src/schema/table_schema.rs` mélange deux catégories :
- **Stratégies auto-inférées** : `Pivot`, `Jsonb`, `StructuredPivot`, `SiblingCollapse`, `AutoSplit`…
- **Overrides manuels IHM/TOML** : `Flatten`, `JsonbFlatten`, `NormalizeDynamicKeys`, `Ignore`

Cycles de vie et sites d'application différents — les mélanger empêche de distinguer "ce que le système a décidé" de "ce que l'utilisateur a demandé".

**Ce qui doit être fait**
- Créer `InferredStrategy` (stratégies auto) et `UserOverride` (overrides manuels)
- `TableSchema` porte les deux champs séparément
- Migrer `config.rs`, `pass2/traversal.rs`, `pass2/insert.rs`, `wide_strategies.rs`
- **Attention** : `InferredStrategy` est sérialisé dans le JSON de persistence — prévoir migration ou bump de version du format

**Fichiers impactés**
- `src/schema/table_schema.rs`, `src/schema/config.rs`, `src/schema/finalizer.rs`
- `src/schema/persistence.rs` (migration format)
- `src/schema/wide_strategies.rs`, `src/pass2/traversal.rs`, `src/pass2/insert.rs`
- `json2sql-ui/` (écran Strategy)

---

### #4 — Rekeyer `NamingRegistry` sur le chemin JSON canonique complet (prérequis multi-fichiers)

**Problème**
La `NamingRegistry` est keyée sur le nom tronqué post-sanitization. En multi-fichiers, deux chemins distincts peuvent collisionner de façon non-déterministe selon l'ordre de traitement.

**Ce qui doit être fait**
- Rekeyer sur le chemin dot-joined canonique complet (ex. `"users.orders.items"`)
- Le nom SQL devient une projection calculée, déterministe indépendamment de l'ordre
- Vérifier les tests `test_dotted_field_name_correct_depth` et `sibling_detection_schema_names_are_deterministic`

**Fichiers impactés**
- `src/schema/naming.rs` (refactor interne)
- `src/schema/finalizer.rs` (API `table_name_from_dot_key` reste stable)

---

### #11 — Nommer explicitement les deux phases de finalisation dans `SchemaFinalizer::run()`

**Problème**
La finalisation a deux phases structurellement différentes (per-table parallèle vs. cross-table séquentielle) dont les contraintes d'ordre ne sont pas expliquées. Les commentaires `// Phase 1/2/3/4` existent mais ne disent pas *pourquoi* cet ordre est contraint.

**Ce qui doit être fait**
- Documenter dans `run()` pourquoi la cascade BFS doit précéder les wide strategies per-table
- Évaluer le renommage de `build_base_schemas` et `apply_wide_table_strategies` pour refléter leur rôle
- Marquer le Problème 3 de `_brain/structure_rework.md` comme partiellement adressé

**Fichiers impactés**
- `src/schema/finalizer.rs` (documentation + éventuellement renommage de fonctions privées)
- `_brain/structure_rework.md`

---

## 🧪 TEST

### #5 — Écrire des tests unitaires pour `pass2/traversal.rs`

**Problème**
`src/pass2/traversal.rs` (582 lignes) n'a **aucun test**. C'est le module qui route les données JSON vers les sinks selon la stratégie — les bugs silencieux ici produisent des `NULL` sans erreur visible.

**Ce qui doit être fait**
Couvrir au minimum :
- `insert_pivot_object()` — insertion key/value Pivot
- `insert_jsonb_object()` — sérialisation JSONB
- `insert_sibling_collapse()` — routing avec clé de sibling
- `insert_structured_pivot_object()` — routing par suffix schema
- `insert_autosplit_object()` — séparation stable/medium columns
- Cas limites : valeur `null`, type mismatch (anomalie), clé absente

**Fichiers impactés**
- `src/pass2/traversal.rs` (section `#[cfg(test)]`)

---

### #6 — Mettre en place des tests d'intégration end-to-end Pass1 + Pass2 + PostgreSQL

**Problème**
Aucun test n'exécute le pipeline complet jusqu'à la base. Les bugs de FK order, de routage de stratégie, ou de COPY silencieux ne sont pas détectables en tests unitaires.

**Ce qui doit être fait**
- Scénarios : table plate, objets imbriqués + FK, SiblingCollapse, wide table
- Utiliser `testcontainers-rs` ou `DATABASE_URL` de CI
- Placer dans `tests/integration/` (feature-gated, ne bloque pas `cargo test` sans PG)

**Fichiers impactés**
- Nouveau répertoire `tests/integration/`
- `Cargo.toml` (dépendance dev `testcontainers`)
- Configuration CI

---

## 🎨 UX

### #7 — Faire remonter les warnings `config.rs` dans l'IHM Dioxus

**Problème**
Les warnings pour tables/colonnes inconnues dans le TOML de config sont émis via `eprintln!` — perdus pour l'utilisateur IHM qui ne voit pas stderr.

**Ce qui doit être fait**
- `apply_overrides()` retourne `Vec<ConfigWarning>` au lieu d'écrire sur stderr
- CLI affiche ces warnings via `eprintln!` ou la progress bar
- IHM les affiche dans l'écran Strategy ou Preview

**Fichiers impactés**
- `src/schema/config.rs` (signature `apply_overrides`)
- `src/pipeline.rs`
- `json2sql-ui/`
- Tests de `config.rs`

---

### #8 — Uniformiser la langue des messages de log (tout en anglais)

**Problème**
`format_flusher_pause_log()` et `format_flusher_resume_log()` dans `src/pass2/runner.rs` produisent des messages en français (`workers pausés`, `workers repris`) alors que le reste des logs est en anglais.

**Ce qui doit être fait**
- Traduire ces deux fonctions en anglais
- Audit rapide des autres `eprintln!` pour détecter d'autres occurrences

**Fichiers impactés**
- `src/pass2/runner.rs`

---

## ✨ FEAT

### #9 — Afficher le compteur d'anomalies par table dans les écrans Strategy et Preview

**Problème**
L'écran Strategy affiche les tables sans indiquer le nombre d'anomalies de coercition. Un utilisateur avec 200 tables ne peut pas prioriser sa review.

**Ce qui doit être fait**
- Agréger `anomaly_iter()` en `HashMap<table_name, u64>` dans `Pass1Result`
- Afficher un badge dans Strategy (ex. "42 anomalies") et un résumé dans Preview
- Ne rien afficher si 0 anomalies

**Fichiers impactés**
- `src/pass1/runner.rs` (ajouter `anomaly_counts` dans `Pass1Result`)
- `src/schema/registry.rs`
- `json2sql-ui/` (écrans Strategy et Preview)

---

## 📄 DOC

### #10 — Mettre à jour `structure_rework.md` : Problème 2 est résolu

**Problème**
`_brain/structure_rework.md` décrit le "Problème 2" (`apply_column_limit_guard` hors pipeline) comme une dette ouverte — mais il est déjà résolu via le flag `apply_pg_guard` dans `SchemaFinalizer::run()` (`src/schema/finalizer.rs:109`).

**Ce qui doit être fait**
- Marquer le Problème 2 comme résolu avec date et référence code
- Vérifier si d'autres points sont partiellement résolus (Problème 3 → commentaires Phase 1/2/3/4)

**Fichiers impactés**
- `_brain/structure_rework.md`
