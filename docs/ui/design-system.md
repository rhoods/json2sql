# Design System — json2sql-ui

## 1. Principe directeur : "The Architectural Logic"

L'interface traite la donnée comme le principal habitant de l'écran. Elle rejette l'esthétique "application web standard" pour un layout éditorial haute densité, à la manière d'un manuel technique soigné ou d'un cockpit professionnel.

La **Premium Density** n'est pas du désordre : c'est une hiérarchie intentionnelle. La profondeur tonale remplace les bordures, les accents monoespace signalent exactement ce qui est "donnée" vs ce qui est "interface".

---

## 2. Tokens de couleur (`src/theme.rs`)

`theme.rs` est la **source de vérité unique** pour toutes les valeurs. Les constantes Rust sont directement interpolées dans les styles (`style: "background:{theme::BG_ROOT};"`) et exposées comme variables CSS via `theme::css()`.

### Hiérarchie de surfaces (règle "No-Line")

Les bordures sont des reliques basse résolution. La logique visuelle est définie par des décalages de fond, pas par des traits.

| Constante Rust       | Valeur    | Rôle                                         |
|----------------------|-----------|----------------------------------------------|
| `BG_ROOT`            | `#131313` | Fond racine — fenêtre de l'application       |
| `BG_WORKSPACE`       | `#1B1B1C` | Espace de travail principal (headers, panels)|
| `BG_SIDEBAR`         | `#2A2A2A` | Sidebars, panneaux d'outils                  |
| `BG_INPUT`           | `#353535` | Champs de saisie, dropdowns                  |
| `BG_EDITOR`          | `#111111` | Log panel, éditeur SQL (plus sombre que root)|

### Texte

| Constante Rust         | Valeur    | Rôle                                  |
|------------------------|-----------|---------------------------------------|
| `ON_SURFACE`           | `#E4E2E6` | Texte principal                       |
| `ON_SURFACE_VARIANT`   | `#C5C6D0` | Texte secondaire                      |
| `ON_SURFACE_DIM`       | `#717680` | Labels, métadonnées, texte désactivé  |
| `ON_PRIMARY`           | `#0D0D0D` | Texte sur fond primaire (contraste)   |

### Tokens sémantiques

| Constante Rust   | Valeur    | Sémantique                        |
|------------------|-----------|-----------------------------------|
| `PRIMARY`        | `#99CBFF` | Action, intention (bleu)          |
| `PRIMARY_DARK`   | `#007BC4` | Fin du dégradé primaire           |
| `SECONDARY`      | `#4EDEA3` | Succès, connexion active (vert)   |
| `SECONDARY_DARK` | `#00C47A` | Fin du dégradé secondaire         |
| `TERTIARY`       | `#FFB95F` | Avertissement, troncature (orange)|
| `ERROR`          | `#FFB4AB` | Erreur, échec de connexion (rouge)|

### Bordures et état

| Constante Rust      | Valeur       | Rôle                                    |
|---------------------|--------------|-----------------------------------------|
| `OUTLINE_VARIANT`   | `#40475266`  | Ghost border (40% opacity) — séparation|
| `OUTLINE_FAINT`     | `#40475233`  | Séparateur de lignes subtil (20%)       |
| `SELECTED_BG`       | `#00A57233`  | Fond ligne sélectionnée (20% secondary) |
| `PRIMARY_ALPHA_08`  | `rgba(153,203,255,0.08)` | Hover fond btn-ghost       |

### Badges de stratégie

| Constante Rust        | Valeur    | Stratégie                              |
|-----------------------|-----------|----------------------------------------|
| `BADGE_DEFAULT`       | `#4A90D9` | DEFAULT (colonnes)                     |
| `BADGE_JSONB`         | `#9B59B6` | JSONB séparé (table propre)            |
| `BADGE_JSONB_OVERFLOW`| `#B8810E` | JSONB auto-converti (>1600 colonnes)   |
| `BADGE_JSONB_INLINE`  | `#16A085` | JSONB inline (colonne parent)          |
| `BADGE_FLATTEN`       | `#27AE60` | FLATTEN / PIVOT / KEYED / MULTI        |
| `BADGE_NORMALIZE`     | `#E67E22` | NORMALIZE / AUTO SPLIT / PIVOT EAV     |
| `BADGE_SKIP`          | `#E74C3C` | SKIP (table exclue)                    |
| `BADGE_ROUTE`         | `#525A65` | ROUTE (routing container — atténué)    |

---

## 3. Typographie

| Famille           | Constante Rust | Usage                                              |
|-------------------|----------------|----------------------------------------------------|
| Inter             | `FONT_UI`      | Interface, labels, boutons                         |
| JetBrains Mono    | `FONT_CODE`    | Noms de tables/colonnes, SQL, JSON, valeurs copiables |

Règle : tout texte copiable/collable dans un terminal utilise `FONT_CODE`. Tout ce qui est interface pure utilise `FONT_UI`.

Tailles courantes :
- Titres de section : `0.875rem`, `font-weight:600`
- Labels ALL-CAPS : `0.6875rem`, `letter-spacing:0.05em`
- Corps / colonnes : `0.8125rem`
- Badges : `0.5625rem` – `0.6875rem`, `font-weight:700`

---

## 4. Élévation et profondeur

On n'utilise pas de drop-shadows pour indiquer la hauteur : on utilise la lumière (décalage de fond).

- **Ghost Border** : si une séparation haute contraste est requise entre deux panneaux sombres, utiliser `OUTLINE_VARIANT` (40% opacity). Ne jamais utiliser une bordure opaque à 100%.
- **Ligne sélectionnée** : `SELECTED_BG` (fond) + `border-left:2px solid SECONDARY` (accent vertical).
- **Modales flottantes** (non utilisées actuellement) : tinted glow `ON_SURFACE` à 6% opacity, blur 32px.

---

## 5. Composants CSS (classes globales)

Les classes sont définies dans `theme::css()` et injectées une seule fois au démarrage. Elles référencent les variables CSS (`var(--token)`), pas de hex codé en dur.

### `.btn-primary`
Dégradé `PRIMARY → PRIMARY_DARK` à 135°, `border-radius:2px`, texte `ON_PRIMARY`.  
`:hover` → `filter:brightness(1.08)` | `[disabled]` → `opacity:0.4`.

### `.btn-ghost`
Fond transparent, texte `PRIMARY`, bordure `OUTLINE_VARIANT`.  
`:hover` → fond `PRIMARY_ALPHA_08` | variante compacte : `.btn-ghost--sm`.

### `.input-field`
Fond `BG_INPUT`, bordure basse uniquement (`OUTLINE_VARIANT`), `border-radius:2px 2px 0 0`.  
Override webkit-autofill inclus pour forcer le fond sombre.

### `.progress-track` / `.progress-bar`
Track : fond `BG_INPUT`, height 6px, `border-radius:3px`.  
Bar : dégradé `SECONDARY → SECONDARY_DARK` horizontal, **pas de border-radius** (style "Brutalist").

### `.log-panel`
Fond `BG_EDITOR`, `FONT_CODE`, couleur `ON_SURFACE_VARIANT`. Utilisé dans Analysis et Import.

---

## 6. Règles d'application

**À faire :**
- Utiliser les tiers `BG_*` pour grouper des éléments liés sans boîtes ni lignes.
- Utiliser `FONT_CODE` pour tout texte qui représente de la donnée (noms de tables, types SQL, chemins).
- Aligner les labels de métadonnées à droite, les données primaires à gauche (asymétrie intentionnelle).

**À ne pas faire :**
- Ne pas utiliser `border-radius > 6px`. Les outils professionnels restent stables avec `2px` ou `4px`.
- Ne pas utiliser du noir pur (`#000`) ou du blanc pur (`#FFF`). Utiliser `BG_ROOT` et `ON_SURFACE`.
- Ne pas utiliser de drop-shadows standard. Si un élément doit ressortir, augmenter son tier `BG_*`.
- Ne pas hardcoder de valeurs hex dans les fichiers de composants — toujours passer par `theme::CONSTANTE`.
