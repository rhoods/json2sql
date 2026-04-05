# Spécification IHM — json2sql-ui

## Vue d'ensemble

`json2sql-ui` est une application web locale (Axum + Leptos) qui permet de visualiser le schéma inféré par la Pass 1 de json2sql, de configurer les stratégies de migration par table/colonne, et de lancer la Pass 2 avec suivi de progression.

**Séparation des responsabilités :**
- **CLI `json2sql`** : Pass 1 (analyse du schéma), Pass 2 (migration), automatisation, pipelines CI
- **IHM `json2sql-ui`** : visualisation du schéma, configuration interactive des stratégies, export TOML, lancement Pass 2

---

## Architecture technique

### Workspace Cargo

```
json2sql/           ← workspace racine
├── json2sql/       ← CLI existant
├── json2sql-ui/    ← nouveau : Axum backend + Leptos frontend
└── json2sql-core/  ← nouveau : types partagés (TableSchema, WideStrategy, SiblingSchema...)
```

`json2sql-core` expose les types Rust partagés entre le CLI et l'IHM — aucune duplication.

### Stack

- **Backend** : Axum (HTTP + Server-Sent Events)
- **Frontend** : Leptos (Rust/WASM, servi par Axum)
- **Communication** : REST pour les actions ponctuelles, SSE pour les flux temps réel
- **Snapshot schema** : chargé en mémoire côté Axum (97MB pour OpenFoodFacts)

### Périmètre V1

L'IHM V1 charge uniquement un snapshot JSON existant généré via `--schema-output`. La Pass 1 reste dans le CLI. La Pass 2 peut être lancée depuis l'IHM avec suivi de progression.

---

## Workflow utilisateur

```
1. CLI : json2sql --input data.jsonl --schema-output schema.json --dry-run
2. IHM : json2sql-ui --snapshot schema.json [--project mon_projet.j2s-project]
3. IHM : visualiser le schéma → configurer les groupes → valider les alertes
4. IHM : exporter le TOML → ou lancer la migration directement
5. CLI (automatisation) : json2sql --project mon_projet.j2s-project
```

---

## Projet `.j2s-project`

Fichier de sauvegarde de session regroupant :
- Chemin du fichier source JSON/NDJSON
- Chemin du snapshot schema
- Paramètres Pass 1 utilisés
- Groupes nommés + membres + stratégies
- Overrides manuels
- Connexion BDD (sans mot de passe)

Sauvegarde automatique à chaque modification. Versionnable en git. Utilisable par le CLI via `--project`.

---

## Layout de l'interface

```
┌─────────────────────────────────────────────────────────────────┐
│  json2sql-ui  [Projet: openfoodfacts]  ⚠️ 3 alertes  [Exporter] │
├──────────────────┬──────────────────────────┬───────────────────┤
│ PANNEAU GAUCHE   │ VUE PRINCIPALE           │ PANNEAU DDL       │
│                  │                          │                   │
│ 🔍 Recherche...  │ [Schéma|Groupes|Décisions│ CREATE TABLE      │
│ Filtres :        │                          │ products_images ( │
│ ⚠️ Alertes       │  products                │   key TEXT,       │
│ ❓ Non configuré │  ├─🟣 images [×47→1]     │   sizes TEXT,     │
│ ✏️ Modifié       │  ├─✅ nutriments         │   uploaded_t INT, │
│ 🤖 Auto-détecté  │  └─🔴 debug_fields [?]  │   uploader TEXT   │
│                  │                          │ );                │
│ ─────────────    │                          │                   │
│ 🟣 images [×47]  │                          │ [Copier] [Tout]   │
│ ✅ nutriments    │                          │                   │
│ 🔴 debug_fields  │                          │                   │
│                  │                          │                   │
│ ─────────────    │                          │                   │
│ ⚠️ ALERTES       │                          │                   │
│ • 3 bloquantes   │                          │                   │
│ • 7 avertiss.    │                          │                   │
└──────────────────┴──────────────────────────┴───────────────────┘
```

### Panneau gauche

- Barre de recherche full-text sur les noms de tables/groupes
- Filtres rapides combinables : `⚠️ Alertes` · `❓ Non configuré` · `✏️ Modifié` · `🤖 Auto-détecté`
- Liste scrollable des tables et groupes avec badges
- Hub d'alertes : compteur global + liste cliquable → focus dans la vue principale
- Panneau fixe, toujours visible

### Vue principale — 3 modes

**Mode Schéma** : arborescence du schéma JSON brut tel qu'inféré par la Pass 1. Navigation, lecture seule.

**Mode Groupes** : accordion des groupes détectés et créés manuellement. Unité de configuration principale.

**Mode Décisions** : arborescence des choix de migration appliqués. Représente ce que sera la BDD finale.

### Panneau DDL

- Affiche le `CREATE TABLE` SQL généré en temps réel pour la table/groupe sélectionné
- Appelle `generate_create_table()` de `db/ddl.rs` via Axum — zéro duplication
- Mis à jour instantanément à chaque changement de stratégie
- Rétractable pour gagner de l'espace

---

## Système de groupes

### Types de groupes

**Table Group** — regroupe des tables sœurs similaires
- Stratégies disponibles : `KeyedPivot` · `Jsonb` · `Pivot` · `Ignore` · `Garder séparé`
- Exemple : `products_images_1`, `_2`, `_fr`, `_en` → groupe `images`

**Column Group** — regroupe des colonnes d'une même table
- Stratégies disponibles : `StructuredPivot` · `Type forcé (INTEGER/TEXT/...)` · `Ignore` · `Garder tel quel`
- Exemple : `calcium_100g`, `calcium_unit`, `calcium_serving` → groupe `calcium`

### Badges visuels

| Badge | Signification |
|---|---|
| 🟣 `KeyedPivot détecté` | Jaccard ≥ 0.7, fusion recommandée |
| 🟡 `Similarité partielle` | Jaccard 0.3–0.7, fusion possible |
| ⚪ `Tables distinctes` | Jaccard < 0.3, garder séparées |
| 🤖 `Auto-détecté` | Groupe créé par la Pass 1 |
| ✏️ `Modifié` | L'utilisateur a changé quelque chose |
| 🆕 `Créé manuellement` | Groupe créé par l'utilisateur |

### Accordion

Les groupes sont pliés par défaut avec un résumé : `products_images [×47 tables] · sizes · uploaded_t · uploader`.
Un clic déplie la liste complète des membres. La stratégie s'applique au groupe entier en un geste.

---

## Paramètres Pass 1

Exposés comme sliders + inputs numériques avec presets :

| Preset | `--wide-column-threshold` | `--sibling-threshold` | `--sibling-jaccard` | `--stable-threshold` | `--rare-threshold` |
|---|---|---|---|---|---|
| Conservateur | 500 | 5 | 0.8 | 0.05 | 0.0001 |
| Défaut | 1000 | 3 | 0.5 | 0.10 | 0.001 |
| Agressif | 20 | 2 | 0.3 | 0.20 | 0.005 |
| Custom | — | — | — | — | — |

Chaque paramètre est individuellement modifiable. Badge `✏️ Modifié` si différent du preset. Bouton `Réinitialiser`. Modifier un paramètre déclenche un re-`finalize()` sur le snapshot en mémoire (pas de relecture du JSON source).

---

## Système d'alertes

### Alertes bloquantes 🔴

Empêchent l'export TOML (bouton grisé avec tooltip explicite) :
- Table sans stratégie assignée
- Conflit BDD bloquant (type incompatible sur colonne existante)
- Groupe incohérent (membres avec schémas trop divergents)

### Avertissements ⚠️

N'empêchent pas l'export mais déclenchent une modale de confirmation :
- Jaccard entre 0.3–0.7 (fusion incertaine)
- Taux d'anomalie élevé sur une colonne
- Table présente en BDD avec schéma différent (non bloquant)

---

## Connexion BDD (optionnelle)

L'IHM fonctionne entièrement sans connexion BDD pour l'analyse et la configuration.

La connexion BDD active deux fonctionnalités :
1. **Conflict Detector** : compare le schéma inféré avec les tables existantes → diff ✅/⚠️/🔴 par table
2. **Lancement Pass 2** : exécute la migration avec suivi de progression SSE

---

## Vue "Informations générales"

Écran d'accueil du projet :

| Métrique | Exemple |
|---|---|
| Lignes analysées | 4 400 000 |
| Tables inférées (brut) | 15 037 |
| Tables après config | 216 |
| Groupes sœurs détectés | 48 |
| Tables larges | 12 |
| Snapshot schema | 97 MB |
| Date de la dernière analyse | 2026-03-23 |
| Paramètres utilisés | `--wide-column-threshold 50 --stable-threshold 0.10` |

---

## Lancement de la migration (Pass 2)

1. Toutes les alertes bloquantes résolues → bouton "Lancer la migration" actif
2. Clic → `POST /api/migration/start` avec connexion BDD + TOML courant
3. Panneau de progression :
   - Barre globale : lignes insérées / total
   - Progression par table (top 10 les plus volumineuses)
   - Logs de flush en temps réel via SSE : `products_images : 100k lignes insérées`
4. Interface non bloquée pendant la migration — navigation possible

---

## API Axum V1

| Méthode | Endpoint | Description |
|---|---|---|
| GET | `/api/schema/tables` | Liste toutes les tables avec stratégie actuelle |
| GET | `/api/schema/tables/:name` | Détail d'une table (colonnes, stats, jaccard) |
| GET | `/api/groups` | Liste des groupes nommés |
| POST | `/api/groups` | Créer un groupe |
| PUT | `/api/groups/:id` | Modifier stratégie ou membres |
| GET | `/api/export/toml` | Générer et télécharger le TOML |
| GET | `/api/export/ddl/:table` | DDL SQL d'une table spécifique |
| POST | `/api/migration/start` | Lancer la Pass 2 |
| GET | `/api/migration/progress` | SSE : progression en temps réel |

---

## Priorités d'implémentation

### Bloc 1 — Core 🔴

1. Extraire `json2sql-core` : `TableSchema`, `WideStrategy`, `SiblingSchema`, `KeyShape`
2. Axum : charger snapshot JSON → `GET /api/schema/tables`
3. Leptos : panneau gauche + liste des tables

### Bloc 2 — Configuration 🔴

4. Groupes nommés : `GET/POST/PUT /api/groups`
5. Vue accordion + badges stratégie dans la vue principale
6. Palette de stratégies contextuelle (Table Group vs Column Group)
7. DDL live : `GET /api/export/ddl/:table` + panneau droit

### Bloc 3 — Validation & Export 🟡

8. Hub alertes + filtres/recherche dans le panneau gauche
9. Export TOML bloqué par alertes : `GET /api/export/toml`
10. Presets paramètres + re-`finalize()` à chaud

### Bloc 4 — Migration 🟡

11. Connexion BDD + Conflict Detector
12. `POST /api/migration/start` + `GET /api/migration/progress` (SSE)
13. Barre de progression Pass 2 + logs live

---

## Fonctionnalités V2+

- **V2** : Lancement Pass 1 depuis l'IHM (sans passer par le CLI)
- **V2** : Historique des décisions — changelog versionné des choix de stratégie
- **V2** : Gestion multi-projets (onglets dans une seule instance Axum)
- **V3+** : Reverse mapping BDD → JSON schema
