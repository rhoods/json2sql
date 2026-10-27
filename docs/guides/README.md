# Guides d'évolution : le format

Les docs fonctionnelles et techniques décrivent **ce qui existe**. Une fiche de ce dossier dit **comment le modifier** :
où intervenir, ce qui casse en silence, quels tests lancer. Elle s'adresse à un développeur comme à un LLM.

Écrire la recette mesure aussi le couplage : une recette qui oblige à toucher douze fichiers est un constat. Chaque
« odeur » relevée (code qui marche mais rend l'évolution coûteuse) peut devenir une issue de refacto. Origine : #96.
Fiche de référence : [ajouter-une-strategie.md](ajouter-une-strategie.md).

## Deux types de fiche

- **carte** : une zone (Pass 1, Pass 2, DB, IHM…) ; rôle, flux, invariants, points d'extension ;
- **recette** : un changement typique (ajouter une stratégie, un type inféré…) ; checklist ordonnée.

## Squelette

```markdown
---
type: recette | carte
sujet: …
verified_at: <commit court>   # la fiche est vraie à ce commit
files:
  - chemins à toucher ou à lire (un dossier finit par /)
symbols:
  - symboles Rust cités dans le texte, méthodes en Type::méthode
---
# Titre
## Rôle                    3 lignes et des liens vers les docs de référence (ne pas les recopier)
## 1. Quel cas ?           (recette) genre de changement → étapes à suivre
## 2. Flux                 du point d'entrée à la sortie, en symboles
## 3. Invariants et pièges ce qui casse sans erreur de compilation
## 4. Recette              ce que le compilateur impose (`match` exhaustifs), puis la checklist de ce qu'il ne voit pas
## 5. Vérifier             commandes exactes
## 6. Odeurs constatées    vu en écrivant, pas tranché → issues de refacto
```

Les listes de l'en-tête s'écrivent une entrée par ligne (`  - x`) : c'est la forme que lit le test.

## Règles

- Citer des symboles, jamais des numéros de ligne.
- Renvoyer aux docs de référence au lieu de les recopier.
- Tout symbole cité dans le texte figure dans `symbols:`. Le test vérifie que tout mot entre backticks (hors blocs
  ```` ``` ````) qui est le nom exact d'une définition du code est listé. Il ne voit pas un symbole cité qui n'existe
  déjà plus : la liste complète, le niveau 1 le signale dès qu'il disparaît.
- Commencer par les zones qui changent le plus (`git log --stat`).

## Garder les fiches à jour

| Niveau | Ce qui est vérifié | Où |
|---|---|---|
| 1. Les références existent | chaque chemin de `files:` existe ; chaque nom de `symbols:` est défini (fn, struct, enum, const, trait, `Type::méthode`) dans `src/`, `json2sql-ui/src/` ou `json2sql-worker/src/` ; tout symbole du code cité dans le texte est dans `symbols:` | `tests/guides_consistency.rs`, en CI |
| 2. Rien n'échappe à la fiche | ce qui s'énumère dans le code est mentionné dans la fiche (pilote : variantes d'`InferredStrategy`, valeurs de `StrategyName::OPTIONAL`) | `tests/guides_consistency.rs`, en CI ; à étendre fiche par fiche |
| 3. Relecture après un changement | une fiche dont un fichier de `files:` a changé depuis `verified_at` est relue | fin de session `brain-dev`, pas en CI |

Niveau 3, à la main :

```bash
git diff --name-only <verified_at>..HEAD -- <files>
```

Si la commande liste des fichiers : mettre la fiche à jour, ou avancer `verified_at` au commit courant, ce qui veut
dire « relue, toujours vraie ». Ce n'est pas bloquant en CI, car une coquille dans un fichier listé suffirait à le
déclencher.

Ajouter une fiche : la créer ici selon le squelette, l'ajouter à la section « Guides d'évolution » de
[docs/index.md](../index.md), lancer `cargo test --test guides_consistency`. Si la fiche suit une liste du code (variantes
d'un enum, valeurs d'une constante), ajouter le test de niveau 2 correspondant dans `tests/guides_consistency.rs`.
