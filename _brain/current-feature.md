# Feature : Script de recherche de valeurs par clé JSON

## Description
Un binaire Rust (`src/bin/json_key_search.rs`) qui lit un fichier NDJSON, cherche récursivement
une clé JSON à toute profondeur dans chaque objet root, et affiche les N premières valeurs
trouvées avec leur chemin JSON (ex: `$.ecoscore_extended_data.impact.data_sources`).

## Motivation
Comprendre le contenu réel d'un champ SiblingCollapse (ex: `data_sources`) avant de décider
quelle colonne SQL ajouter en remplacement de `j2s_data`. Outil de debug/exploration one-shot.

## Interface CLI
```
cargo run --bin json_key_search -- --key <nom> --limit <N> <fichier.ndjson>
```
- `--key` : nom de la clé JSON à chercher (requis)
- `--limit` : nombre max d'occurrences à afficher (défaut : 10)
- `<fichier>` : fichier NDJSON en argument positionnel

## Output attendu
Pour chaque occurrence :
```
[1] $.path.to.key = "valeur"
[2] $.other.path.key = { "obj": true }
```

## Modules / fichiers impactés
- `src/bin/json_key_search.rs` — nouveau fichier uniquement (aucun module existant touché)

## Tâches de développement
1. Implémenter `src/bin/json_key_search.rs` : parse args (`--key`, `--limit`, fichier positionnel),
   lecture NDJSON streamée ligne par ligne, recherche récursive avec tracking du chemin,
   affichage valeur + chemin, arrêt après N occurrences

## Impacts et edge cases
_À compléter à l'étape 2_

## Documentation
_À compléter à l'étape 3_
