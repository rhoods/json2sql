# json2sql — Documentation Index

Point d'entrée unique. Chaque entrée indique **quand consulter** le document.

---

## Fonctionnel

| Document | Consulter pour... |
|---|---|
| [functional/overview.md](functional/overview.md) | Comprendre ce que fait l'outil, ses choix de conception (deux passes, normalisation, types inférés, anomalies) |
| [functional/usage.md](functional/usage.md) | Utiliser l'outil : CLI flags, exemples, variable d'environnement, config TOML, cas concrets |

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
| [ui/ihm-design-prompt.md](ui/ihm-design-prompt.md) | Prompt Stitch pour (re)générer les maquettes des 5 écrans |

---

## Archives

| Dossier | Contenu |
|---|---|
| [`_archive/specs/`](../_archive/specs/) | Specs de features issues des sessions brainstorming (état d'implémentation non vérifié) |
| [`_bmad-output/brainstorming/`](../_bmad-output/brainstorming/) | Sessions de brainstorming brutes (artefacts de processus) |
