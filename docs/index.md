# json2sql — Documentation Index

Point d'entrée unique. Chaque entrée indique **quand consulter** le document.

---

## Fonctionnel

| Document | Consulter pour... |
|---|---|
| [functional/overview.md](functional/overview.md) | Comprendre ce que fait l'outil, ses choix de conception (deux passes, normalisation, types inférés, anomalies) |
| [functional/usage.md](functional/usage.md) | Utiliser l'outil : CLI flags, exemples, variable d'environnement, config TOML, cas concrets |
| [functional/strategies.md](functional/strategies.md) | Voir, stratégie par stratégie, le schéma SQL produit avec et sans elle (exemples reproductibles dans `examples/strategies/`) |

---

## Technique

| Document | Consulter pour... |
|---|---|
| [technical/architecture.md](technical/architecture.md) | Vue d'ensemble du pipeline Pass1/Pass2, rôle détaillé de chaque module et fichier `src/`, protocole d'événements IHM, persistance du schéma, tests |
| [technical/modules.md](technical/modules.md) | Référence rapide : rôle d'un fichier ou dossier spécifique dans `src/` |
| [technical/schema-inference.md](technical/schema-inference.md) | Heuristiques d'inférence dans l'ordre d'exécution : types, tables larges, AutoSplit, StructuredPivot, KeyedPivot, sanitisation, surcharges TOML |

---

## IHM (json2sql-ui)

| Document | Consulter pour... |
|---|---|
| [ui/design-system.md](ui/design-system.md) | Tokens, palette, typographie, règles visuelles du design system "The Architectural Logic" |
| [ui/architecture.md](ui/architecture.md) | Structure des composants, AppState, écrans, composants partagés (TableListPanel, StrategyConfigurator), règles d'architecture |
| [ui/ihm-design-prompt.md](ui/ihm-design-prompt.md) | Prompt Stitch pour (re)générer les maquettes des 5 écrans |

---

## Guides d'évolution

Fiches « comment modifier » : où intervenir, pièges, tests à lancer. À relire avant de toucher la zone concernée.

Format et contrôle : [guides/README.md](guides/README.md). Après un changement dans une zone, relire les fiches dont un
fichier de `files:` a changé depuis `verified_at` (`git diff --name-only <verified_at>..HEAD -- <files>`) : mettre à jour la
fiche ou avancer `verified_at`. `tests/guides_consistency.rs` vérifie en CI que les références des fiches existent.

| Document | Consulter pour... |
|---|---|
| [guides/ajouter-une-strategie.md](guides/ajouter-une-strategie.md) | Ajouter une stratégie de matérialisation : tableau des stratégies existantes, flux, checklist (ce que le compilateur impose, et le reste), odeurs constatées (fiche pilote) |
| [guides/cli-pipeline.md](guides/cli-pipeline.md) | Modifier la CLI ou l'enchaînement d'un import (options, modes, étapes sur le schéma, rapports) : où intervenir, pièges silencieux (option non recopiée dans `main`, chaînes CLI et IHM séparées, stdout réservé aux résultats) |
| [guides/finalisation.md](guides/finalisation.md) | Modifier la finalisation du schéma (phases de `SchemaFinalizer`, fusion des tables sœurs, noms, garde des 1 600 colonnes) : où intervenir, formes de clé, pièges silencieux (tables synthétiques, dédoublonnage, colonne clé), checklists |
| [guides/pass1.md](guides/pass1.md) | Modifier la Pass 1 (lecture du fichier, observation des types, échantillon) : où intervenir, tableau des types inférés, pièges silencieux (fusion parallèle, ordre des colonnes, nouveau type qui devient `TEXT`), checklists statistique et type inféré |
| [guides/pass2.md](guides/pass2.md) | Modifier la Pass 2 (parcours du JSON, écriture des lignes, COPY) : où intervenir, pièges silencieux (ordre des colonnes, tables et sinks sautés, colonnes générées), tests sans base avec `CaptureSink` |
| [guides/worker-ipc.md](guides/worker-ipc.md) | Modifier le worker de l'IHM ou son protocole (config sur stdin, socket, fichier résultat) : où intervenir, événements `ProgressEvent` et leur rôle, pièges silencieux (fin du flux à `Pass2Done`, commande d'annulation en dur, reprise), points d'extension |

---

## Archives

| Dossier | Contenu |
|---|---|
| [`_archive/specs/`](../_archive/specs/) | Specs de features issues des sessions brainstorming (état d'implémentation non vérifié) |
| [`_bmad-output/brainstorming/`](../_bmad-output/brainstorming/) | Sessions de brainstorming brutes (artefacts de processus) |
