//! Vérifie que les fiches de `docs/guides/` restent vraies face au code (issue #96, niveaux 1 et 2
//! de `docs/guides/README.md`).
//!
//! - Niveau 1 : chaque chemin de `files:` existe ; chaque nom de `symbols:` est défini (fn,
//!   struct, enum, const, trait, méthode `Type::nom`) dans `src/`, `json2sql-ui/src/` ou
//!   `json2sql-worker/src/`. L'extraction est celle de `tests/header_consistency.rs` (#33),
//!   partagée dans `tests/common/code_items.rs`. Filet : un mot cité entre backticks dans le texte
//!   qui est le nom exact d'une définition doit figurer dans `symbols:`.
//! - Niveau 2 : ce qui s'énumère dans le code est mentionné dans la fiche. Pilote : variantes
//!   d'`InferredStrategy` et valeurs de `StrategyName::OPTIONAL` (fiche `ajouter-une-strategie.md`),
//!   variantes d'`InferredType` (fiche `pass1.md`), variantes de `KeyShape` et `ChildKind`
//!   (fiche `finalisation.md`), variantes de `ProgressEvent`
//!   (fiche `worker-ipc.md`), options de la CLI et leurs valeurs
//!   dans `usage.md` (fiche `cli-pipeline.md`), variantes de `PgType` (fiche `db.md`), variantes de `Signal`
//!   (fiche `groupes-colonnes.md`), variantes d'`AppScreen` (fiche `ihm.md`).
//!
//! L'en-tête YAML est lu à la main (listes `- x` sous une clé) : pas de dépendance YAML pour un
//! format aussi simple.
#![allow(clippy::disallowed_methods)]

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use json2sql::schema::strategies::StrategyName;

#[path = "common/code_items.rs"]
mod code_items;
use code_items::{collect_rs_files, extract_code_items, find_matching_brace};

const STRATEGY_GUIDE: &str = "docs/guides/ajouter-une-strategie.md";
const PASS1_GUIDE: &str = "docs/guides/pass1.md";
const FINALISATION_GUIDE: &str = "docs/guides/finalisation.md";
const IHM_GUIDE: &str = "docs/guides/ihm.md";
const GROUPS_GUIDE: &str = "docs/guides/groupes-colonnes.md";
const DB_GUIDE: &str = "docs/guides/db.md";
const WORKER_IPC_GUIDE: &str = "docs/guides/worker-ipc.md";

/// En-tête YAML d'une fiche : `verified_at` et les listes `files:` / `symbols:`.
#[derive(Debug, Default, PartialEq)]
struct FrontMatter {
    verified_at: Option<String>,
    files: Vec<String>,
    symbols: Vec<String>,
}

/// Lit l'en-tête entre les deux premières lignes `---`. `None` si la fiche n'en a pas.
fn front_matter(content: &str) -> Option<FrontMatter> {
    let mut lines = content.lines();
    if lines.next()? != "---" {
        return None;
    }
    let mut fm = FrontMatter::default();
    let mut current: Option<&str> = None;
    for line in lines {
        if line == "---" {
            return Some(fm);
        }
        if let Some(item) = line.trim_start().strip_prefix("- ") {
            let item = item.trim().to_string();
            match current {
                Some("files") => fm.files.push(item),
                Some("symbols") => fm.symbols.push(item),
                _ => {}
            }
        } else if let Some((key, value)) = line.split_once(':') {
            current = Some(key.trim());
            if key.trim() == "verified_at" {
                // `verified_at: abc1234   # commentaire`
                let v = value.split('#').next().unwrap_or("").trim();
                fm.verified_at = (!v.is_empty()).then(|| v.to_string());
            }
        }
    }
    None // pas de `---` fermant
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Fiches de `docs/guides/`, hors `README.md` (qui décrit le format).
fn guide_paths(root: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(root.join("docs/guides"))
        .expect("docs/guides/ doit exister")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "md"))
        .filter(|p| p.file_name().is_some_and(|n| n != "README.md"))
        .collect();
    paths.sort();
    paths
}

/// Noms de toutes les définitions Rust des trois crates (nature ignorée).
fn defined_names(root: &Path) -> HashSet<String> {
    let dirs = [
        root.join("src"),
        root.join("json2sql-ui/src"),
        root.join("json2sql-worker/src"),
    ];
    let dir_refs: Vec<&Path> = dirs.iter().map(PathBuf::as_path).collect();
    collect_rs_files(&dir_refs)
        .iter()
        .filter_map(|p| fs::read_to_string(p).ok())
        .flat_map(|c| extract_code_items(&c))
        .map(|item| item.name)
        .collect()
}

/// Niveau 1 : chaque référence d'en-tête existe. Agrège toutes les erreurs en un seul échec.
#[test]
fn guide_references_exist() {
    let root = workspace_root();
    let names = defined_names(&root);
    let mut errors = Vec::new();
    for path in guide_paths(&root) {
        let display = path.strip_prefix(&root).unwrap_or(&path).display().to_string();
        let content = fs::read_to_string(&path).unwrap();
        let Some(fm) = front_matter(&content) else {
            errors.push(format!("  {display}: pas d'en-tête YAML entre deux lignes `---`"));
            continue;
        };
        if fm.verified_at.is_none() {
            errors.push(format!("  {display}: `verified_at` manquant"));
        }
        for file in &fm.files {
            if !root.join(file).exists() {
                errors.push(format!("  {display}: files: `{file}` n'existe pas"));
            }
        }
        for symbol in &fm.symbols {
            if !names.contains(symbol) {
                errors.push(format!("  {display}: symbols: `{symbol}` n'est défini nulle part"));
            }
        }
    }
    assert!(
        errors.is_empty(),
        "Fiches de docs/guides/ désynchronisées du code :\n{}",
        errors.join("\n")
    );
}

/// Mots entre backticks simples du corps de la fiche (après l'en-tête), hors blocs ```.
fn cited_spans(content: &str) -> Vec<String> {
    // Le `---` d'ouverture n'a pas de `\n` devant : le premier `\n---\n` est la fermeture.
    let body = content.split_once("\n---\n").map_or(content, |(_, body)| body);
    let mut in_fence = false;
    let mut spans = Vec::new();
    for line in body.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }
        // Les segments impairs d'un split sur '`' sont entre backticks.
        spans.extend(line.split('`').skip(1).step_by(2).map(str::to_string));
    }
    spans
}

/// Filet du niveau 1 : un mot cité entre backticks qui est exactement le nom d'une définition
/// du code doit figurer dans `symbols:`, sinon le niveau 1 ne le surveille pas. Ne voit pas les
/// symboles déjà disparus du code (ils ne sont plus « définis ») : c'est le niveau 1 qui les
/// attrape, une fois la liste complète.
#[test]
fn cited_symbols_are_listed() {
    let root = workspace_root();
    let names = defined_names(&root);
    let mut errors = Vec::new();
    for path in guide_paths(&root) {
        let display = path.strip_prefix(&root).unwrap_or(&path).display().to_string();
        let content = fs::read_to_string(&path).unwrap();
        let Some(fm) = front_matter(&content) else {
            continue; // signalé par `guide_references_exist`
        };
        let listed: HashSet<&str> = fm.symbols.iter().map(String::as_str).collect();
        let mut missing: Vec<String> = cited_spans(&content)
            .into_iter()
            .filter(|s| names.contains(s) && !listed.contains(s.as_str()))
            .collect();
        missing.sort();
        missing.dedup();
        for s in missing {
            errors.push(format!("  {display}: `{s}` cité dans le texte mais absent de symbols:"));
        }
    }
    assert!(
        errors.is_empty(),
        "Symboles cités non listés dans l'en-tête :\n{}",
        errors.join("\n")
    );
}

/// Variantes d'un enum, lues dans le source : identifiants en majuscule à 4 espaces
/// d'indentation dans le corps de l'enum (les champs des variantes sont à 8, `Null = 0,` donne `Null`).
fn enum_variants(root: &Path, file: &str, enum_name: &str) -> Vec<String> {
    let src = fs::read_to_string(root.join(file)).unwrap();
    let decl = format!("pub enum {enum_name}");
    let start = src
        .find(&decl)
        .unwrap_or_else(|| panic!("`{decl}` introuvable dans {file}"));
    let open = start + src[start..].find('{').unwrap();
    let close = find_matching_brace(src.as_bytes(), open).unwrap();
    src[open + 1..close]
        .lines()
        .filter_map(|line| line.strip_prefix("    "))
        .filter(|rest| rest.starts_with(|c: char| c.is_ascii_uppercase()))
        .map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect()
        })
        .collect()
}

/// Variantes d'un enum sans ligne `| `Variante` |` dans un tableau de la fiche.
fn variants_missing_from_table(guide: &str, variants: &[String]) -> Vec<String> {
    variants
        .iter()
        .filter(|v| !guide.lines().any(|l| l.starts_with(&format!("| `{v}` |"))))
        .cloned()
        .collect()
}

/// Niveau 2 : chaque variante d'`InferredStrategy` a sa ligne dans le tableau de la fiche.
#[test]
fn strategy_guide_lists_every_inferred_strategy() {
    let root = workspace_root();
    let guide = fs::read_to_string(root.join(STRATEGY_GUIDE)).unwrap();
    let variants = enum_variants(&root, "src/schema/table_schema.rs", "InferredStrategy");
    assert!(variants.len() > 1, "extraction des variantes cassée : {variants:?}");
    let missing = variants_missing_from_table(&guide, &variants);
    assert!(
        missing.is_empty(),
        "{STRATEGY_GUIDE} : pas de ligne `| `Variante` |` dans le tableau du § 1 pour {missing:?}"
    );
}

/// Niveau 2 : chaque variante d'`InferredType` a sa ligne dans le tableau des types de la fiche
/// Pass 1. Lu dans le source, pas dans `InferredType::ALL` : l'assertion `const` de
/// `type_tracker.rs` ne voit pas une variante ajoutée sans `COUNT` ni `ALL`.
#[test]
fn pass1_guide_lists_every_inferred_type() {
    let root = workspace_root();
    let guide = fs::read_to_string(root.join(PASS1_GUIDE)).unwrap();
    let variants = enum_variants(&root, "src/schema/type_tracker.rs", "InferredType");
    assert!(variants.len() > 1, "extraction des variantes cassée : {variants:?}");
    let missing = variants_missing_from_table(&guide, &variants);
    assert!(
        missing.is_empty(),
        "{PASS1_GUIDE} : pas de ligne `| `Variante` |` dans le tableau des types pour {missing:?}"
    );
}

/// Niveau 2 : chaque variante de `KeyShape` et de `ChildKind` a sa ligne dans un tableau de la
/// fiche finalisation. Le nom de colonne clé est choisi par des `match` avec cas par défaut : une
/// forme de clé ajoutée compile sans qu'on y pense.
#[test]
fn finalisation_guide_lists_key_shapes_and_child_kinds() {
    let root = workspace_root();
    let guide = fs::read_to_string(root.join(FINALISATION_GUIDE)).unwrap();
    for enum_name in ["KeyShape", "ChildKind"] {
        let variants = enum_variants(&root, "src/schema/table_schema.rs", enum_name);
        assert!(variants.len() > 1, "extraction des variantes de {enum_name} cassée : {variants:?}");
        let missing = variants_missing_from_table(&guide, &variants);
        assert!(
            missing.is_empty(),
            "{FINALISATION_GUIDE} : pas de ligne `| `Variante` |` dans le tableau {enum_name} pour {missing:?}"
        );
    }
}

/// Niveau 2 : chaque variante de `ProgressEvent` a sa ligne dans le tableau des événements de
/// la fiche worker et IPC (passe ou non par le socket, rôle dans le protocole).
#[test]
fn worker_ipc_guide_lists_every_progress_event() {
    let root = workspace_root();
    let guide = fs::read_to_string(root.join(WORKER_IPC_GUIDE)).unwrap();
    let variants = enum_variants(&root, "src/io/progress_event.rs", "ProgressEvent");
    assert!(variants.len() > 1, "extraction des variantes cassée : {variants:?}");
    let missing = variants_missing_from_table(&guide, &variants);
    assert!(
        missing.is_empty(),
        "{WORKER_IPC_GUIDE} : pas de ligne `| `Variante` |` dans le tableau des événements pour {missing:?}"
    );
}

/// Lignes de tableau `| `--x` |` d'un texte, par nom d'option.
fn option_row<'a>(text: &'a str, long: &str) -> Option<&'a str> {
    let start = format!("| `--{long}` |");
    text.lines().find(|l| l.starts_with(&start))
}

/// Niveau 2 (fiche `cli-pipeline.md`) : chaque option de la CLI a sa ligne dans usage.md, avec ses
/// valeurs possibles ; les options d'`inspect` dans la section de `inspect`. Lu par l'introspection
/// de clap (`Cli::command()`), dérivée des définitions, pas d'une liste tenue à la main.
#[test]
fn cli_options_documented() {
    use clap::CommandFactory;
    let usage = fs::read_to_string(workspace_root().join("docs/functional/usage.md")).unwrap();
    let inspect_section = usage
        .split_once("### Paramètres de `inspect`")
        .and_then(|(_, rest)| rest.split("\n### ").next())
        .expect("section « Paramètres de `inspect` » introuvable dans usage.md");
    let cli = json2sql::cli::Cli::command();
    let inspect = cli.find_subcommand("inspect").expect("sous-commande `inspect` introuvable");

    let mut errors = Vec::new();
    let mut checked = 0;
    for (cmd, text, where_) in [(&cli, usage.as_str(), "usage.md"), (inspect, inspect_section, "usage.md § inspect")] {
        for arg in cmd.get_arguments() {
            let Some(long) = arg.get_long() else { continue }; // `FILE` d'inspect, positionnel
            if long == "help" || long == "version" {
                continue;
            }
            checked += 1;
            let Some(row) = option_row(text, long) else {
                errors.push(format!("  {where_} : pas de ligne `| `--{long}` |`"));
                continue;
            };
            // Les booléens ont `true`/`false` pour valeurs : seules comptent les options à valeur.
            let values = if arg.get_action().takes_values() { arg.get_possible_values() } else { Vec::new() };
            for value in values {
                if !row.contains(&format!("`{}`", value.get_name())) {
                    errors.push(format!("  {where_} : `{}` absent de la ligne `--{long}`", value.get_name()));
                }
            }
        }
    }
    assert!(checked > 20, "extraction des options cassée : {checked} options");

    // L'aide de `--disable-strategy` recopie les valeurs de `StrategyName::OPTIONAL` (odeur 4 de #96).
    let help = cli
        .get_arguments()
        .find(|a| a.get_long() == Some("disable-strategy"))
        .and_then(|a| a.get_help())
        .map(ToString::to_string)
        .unwrap_or_default();
    for s in StrategyName::OPTIONAL {
        if !help.contains(s.as_str()) {
            errors.push(format!("  src/cli.rs : `{}` absent de l'aide de `--disable-strategy`", s.as_str()));
        }
    }
    assert!(errors.is_empty(), "Options de la CLI non documentées :\n{}", errors.join("\n"));
}

/// Niveau 2 : chaque variante de `PgType` a sa ligne dans le tableau des types de la fiche base
/// de données (rendu DDL, accepté ou non dans un override TOML). `PgType::as_sql` est protégé
/// par le compilateur, `parse_pg_type` non : la ligne dit s'il faut aussi le toucher.
#[test]
fn db_guide_lists_every_pg_type() {
    let root = workspace_root();
    let guide = fs::read_to_string(root.join(DB_GUIDE)).unwrap();
    let variants = enum_variants(&root, "src/schema/type_tracker.rs", "PgType");
    assert!(variants.len() > 1, "extraction des variantes cassée : {variants:?}");
    let missing = variants_missing_from_table(&guide, &variants);
    assert!(
        missing.is_empty(),
        "{DB_GUIDE} : pas de ligne `| `Variante` |` dans le tableau des types pour {missing:?}"
    );
}

/// Niveau 2 : chaque variante de `Signal` a sa ligne dans le tableau des signaux de la fiche des
/// groupes de colonnes. `DataSignals::evidence` recopie ces variantes à la main, ce que le compilateur
/// ne vérifie pas : la fiche le rappelle.
#[test]
fn groups_guide_lists_every_signal() {
    let root = workspace_root();
    let guide = fs::read_to_string(root.join(GROUPS_GUIDE)).unwrap();
    let variants = enum_variants(&root, "src/schema/grouping/proposal.rs", "Signal");
    assert!(variants.len() > 1, "extraction des variantes cassée : {variants:?}");
    let missing = variants_missing_from_table(&guide, &variants);
    assert!(
        missing.is_empty(),
        "{GROUPS_GUIDE} : pas de ligne `| `Variante` |` dans le tableau des signaux pour {missing:?}"
    );
}

/// Niveau 2 : chaque variante d'`AppScreen` a sa ligne dans le tableau des écrans de la fiche IHM.
#[test]
fn ihm_guide_lists_every_app_screen() {
    let root = workspace_root();
    let guide = fs::read_to_string(root.join(IHM_GUIDE)).unwrap();
    let variants = enum_variants(&root, "json2sql-ui/src/state/mod.rs", "AppScreen");
    assert!(variants.len() > 1, "extraction des variantes cassée : {variants:?}");
    let missing = variants_missing_from_table(&guide, &variants);
    assert!(
        missing.is_empty(),
        "{IHM_GUIDE} : pas de ligne `| `Variante` |` dans le tableau des écrans pour {missing:?}"
    );
}

/// Niveau 2 : chaque stratégie désactivable figure dans la colonne « Désactivable » du tableau
/// de la fiche et sur la ligne `--disable-strategy` d'`usage.md`.
#[test]
fn optional_strategies_documented() {
    let root = workspace_root();
    let guide = fs::read_to_string(root.join(STRATEGY_GUIDE)).unwrap();
    // 4e cellule des lignes du tableau des stratégies.
    let disable_column: String = guide
        .lines()
        .filter(|l| l.starts_with("| `"))
        .filter_map(|l| l.split('|').nth(4))
        .collect();
    let usage = fs::read_to_string(root.join("docs/functional/usage.md")).unwrap();
    let usage_line = usage
        .lines()
        .find(|l| l.starts_with("| `--disable-strategy`"))
        .expect("ligne `--disable-strategy` introuvable dans usage.md");

    let mut errors = Vec::new();
    for s in StrategyName::OPTIONAL {
        let name = format!("`{}`", s.as_str());
        if !disable_column.contains(&name) {
            errors.push(format!("  {STRATEGY_GUIDE} : {name} absent de la colonne « Désactivable »"));
        }
        if !usage_line.contains(&name) {
            errors.push(format!("  usage.md : {name} absent de la ligne `--disable-strategy`"));
        }
    }
    assert!(errors.is_empty(), "Stratégies désactivables non documentées :\n{}", errors.join("\n"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn front_matter_reads_lists_and_ignores_other_keys() {
        let content = "---\ntype: recette\nverified_at: abc1234   # commentaire\nfiles:\n  - src/a.rs\n  - docs/\nautre:\n  - ignoré\nsymbols:\n  - Foo::bar\n---\n# Titre\n";
        assert_eq!(
            front_matter(content),
            Some(FrontMatter {
                verified_at: Some("abc1234".into()),
                files: vec!["src/a.rs".into(), "docs/".into()],
                symbols: vec!["Foo::bar".into()],
            })
        );
    }

    #[test]
    fn cited_spans_skips_header_and_fenced_blocks() {
        let content = "---\nsymbols:\n  - `entete`\n---\n# `Titre`\nA `un` et `deux`.\n```bash\n`bloc`\n```\n---\nfin `trois`\n";
        assert_eq!(cited_spans(content), vec!["Titre", "un", "deux", "trois"]);
    }

    #[test]
    fn enum_variants_reads_explicit_discriminants() {
        let variants = enum_variants(&workspace_root(), "src/schema/type_tracker.rs", "InferredType");
        assert_eq!(variants.first().map(String::as_str), Some("Null"));
        assert!(variants.contains(&"Array".to_string()), "{variants:?}");
    }

    #[test]
    fn front_matter_requires_opening_and_closing_markers() {
        assert_eq!(front_matter("# Titre\n"), None);
        assert_eq!(front_matter("---\nfiles:\n  - a\n"), None);
    }
}
