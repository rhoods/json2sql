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

## 5. Sources CSS

| Source | Chargée par | Contenu |
|---|---|---|
| `assets/styles.css` | `theme::design_css()` → `include_str!` | Design system complet (1046 lignes) — layouts, composants, utilitaires |
| `theme::css()` | inline | Variables CSS `:root`, `.btn-primary`, `.btn-ghost`, `.input-field`, `.progress-track`, `.log-panel`, overrides webkit |

Les deux sont injectées dans un `<style>` unique dans le `<head>` du webview au démarrage. `assets/styles.css` est la source à éditer pour tout nouveau composant.

---

## 6. Composants CSS — `assets/styles.css`

### Layouts

#### `.split-3`
Layout trois panneaux flex avec splitters drag-and-drop.  
Panneaux enfants : `.pane` (largeur fixe) ou `.pane.fluid` (flex:1). Dernier `.pane` sans bordure droite.  
`.pane.collapsed` → repli avec `.collapsed-strip` (bande verticale avec label rotaté).

#### `.split-60-40`
Layout deux panneaux 60/40 fixe. Utilisé par Analysis et Import.  
Panneaux : `.pane` (fond `--bg-sidebar`).

#### `.pane`
Conteneur de panneau : flex column, `overflow:hidden`, bordure droite `--bd`.  
En-tête : `.pane-head` (hauteur 36px, flex, fond `--bg-2`). Corps : `.pane-body` (overflow auto, padding 12px) ou `.pane-body.no-pad`.

#### `.subbar`
Barre de navigation secondaire sous le titlebar. Contient `.crumb` (breadcrumb), `.crumb .step`, `.crumb .step.active`, et `.stat-row`.

### Écran 1 — Wizard

#### `.step-card`
Carte d'étape accordion. États : `.done` (bordure verte), `.active` (bordure accent + glow, corps visible), `.todo` (opacity 0.85, corps masqué).  
`.step-head` (en-tête cliquable) + `.step-body` (corps masqué par défaut, visible si `.active`).

### Écran 2 — Analysis

#### `.stat-tile`
Tuile de statistique. `.lbl` (label ALL-CAPS), `.val` (valeur monospace 28px), `.sub` (sous-label monospace xs).  
Variantes : `.stat-tile.warn` (valeur orange), `.stat-tile.acc` (valeur bleue).

### Écrans 3 & 4 — Strategy / Preview

#### `.strat-list` / `.strat-btn`
Liste et boutons de sélection de stratégie (flex column, gap 4px).  
`.strat-btn` : fond `--bg`, bordure `--bd`, radius `--r-md`, flex row.  
`.strat-btn.on` : fond accent alpha, bordure accent — état sélectionné.  
`.strat-btn .nm` (label flex:1) + `.strat-btn .dsc` (description secondaire) + `.strat-btn .radio` (indicateur rond).

#### `table.t`
Table de données dense. `thead th` : texte xs ALL-CAPS, `--fg-3`, fond `--bg-2`, sticky.  
`tbody td` : texte xs, bordure basse `--bd`, padding `5px 10px`.  
`tr:hover` → `--bg-2` | `tr.sel` → fond accent alpha + `box-shadow inset 2px` accent gauche.

#### `.badge`
Badge inline. Fond translucide + bordure colorée selon variante.  
Variantes sémantiques : `.badge.default`, `.badge.jsonb`, `.badge.jsonbi`, `.badge.flatten`, `.badge.normalize`, `.badge.pivot`, `.badge.skip`.  
Variantes état : `.badge.warn`, `.badge.danger`, `.badge.success`, `.badge.info`, `.badge.muted`, `.badge.acc`.  
Variante forme : `.badge.sq` (border-radius réduit).

### Écrans 2 & 5 — Logs

#### `.log`
Panneau de log monospace. Fond `--bg-editor`, `overflow-y:auto`.  
Colorisation des lignes :  
- `.log .ts` → gris faint (timestamp)  
- `.log .warn` → orange  
- `.log .err` → rouge  
- `.log .ok` → vert  
- `.log .keyw` → bleu info  
- `.log .num` → accent

### Écrans 2 & 5 — Progress

#### `.prog`
Barre de progression. Hauteur 6px, fond `--bg-3`, dégradé vert `--success → --success-dark` (enfant `i`).  
`.prog.thick` → hauteur 10px.  
`.prog.indeterminate` → animation de balayage gauche→droite.  
`.prog.warn` → dégradé orange | `.prog.danger` → dégradé rouge.

### Écran 4 — DDL Preview

#### `pre.code` / `.code`
Bloc code monospace, fond `--bg-editor`, `white-space:pre-wrap`.  
Tokens DDL SQL :  
- `.code .kw` → violet (`#b48cf0`, bold) — mots-clés SQL (`CREATE`, `TABLE`, `NOT NULL`, etc.)  
- `.code .ty` → cyan (`#4dd0c9`) — types PG (`TEXT`, `INTEGER`, `JSONB`, etc.)  
- `.code .pn` → blanc bold — noms de tables/colonnes  
- `.code .num` → orange — littéraux numériques  
- `.code .str` → vert — littéraux chaîne  
- `.code .com` → gris italique — commentaires

### Utilitaires

| Classe | Rôle |
|---|---|
| `.row` | flex row, gap 8px, align center |
| `.col` | flex column, gap 8px |
| `.grow` | flex:1 |
| `.gap-sm/md/lg/xl` | gap 4/8/14/20px |
| `.fg-2/3/4` | couleurs texte secondaires |
| `.fs-xs/sm/lg` | tailles de police |
| `.w-100` | width:100% |
| `.mt/mb-sm/md/lg` | marges top/bottom |
| `.ta-r/.ta-c` | alignement texte |
| `.divider` | séparateur horizontal 1px `--bd` |
| `.divider-v` | séparateur vertical 1px `--bd` |
| `.cdot` | indicateur de statut rond — `.cdot.ok` (vert), `.cdot.warn` (orange), `.cdot.err` (rouge) |
| `.mono` | force JetBrains Mono |

### Héritage — `theme::css()`

Classes historiques encore utilisées dans les composants inline :

| Classe | Rôle |
|---|---|
| `.btn-primary` | Dégradé `PRIMARY → PRIMARY_DARK`, texte `ON_PRIMARY` |
| `.btn-ghost` / `.btn-ghost--sm` | Fond transparent, texte `PRIMARY`, bordure ghost |
| `.input-field` | Fond `BG_INPUT`, bordure basse uniquement, override webkit-autofill |
| `.progress-track` / `.progress-bar` | Track 6px + bar dégradé vert |
| `.log-panel` | Fond `BG_EDITOR`, FONT_CODE |

---

## 7. Règles d'application

**À faire :**
- Utiliser les tiers `BG_*` pour grouper des éléments liés sans boîtes ni lignes.
- Utiliser `FONT_CODE` pour tout texte qui représente de la donnée (noms de tables, types SQL, chemins).
- Aligner les labels de métadonnées à droite, les données primaires à gauche (asymétrie intentionnelle).

**À ne pas faire :**
- Ne pas utiliser `border-radius > 6px`. Les outils professionnels restent stables avec `2px` ou `4px`.
- Ne pas utiliser du noir pur (`#000`) ou du blanc pur (`#FFF`). Utiliser `BG_ROOT` et `ON_SURFACE`.
- Ne pas utiliser de drop-shadows standard. Si un élément doit ressortir, augmenter son tier `BG_*`.
- Ne pas hardcoder de valeurs hex dans les fichiers de composants — toujours passer par `theme::CONSTANTE`.
