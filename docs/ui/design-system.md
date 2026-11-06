# Design System — json2sql-ui

## 1. Principe directeur : "The Architectural Logic"

L'interface traite la donnée comme le principal habitant de l'écran. Elle rejette l'esthétique "application web standard" pour un layout éditorial haute densité, à la manière d'un manuel technique soigné ou d'un cockpit professionnel.

La **Premium Density** n'est pas du désordre : c'est une hiérarchie intentionnelle. La profondeur tonale remplace les bordures, les accents monoespace signalent exactement ce qui est "donnée" vs ce qui est "interface".

---

## 2. Tokens (`assets/styles.css`)

Les tokens sont des variables CSS, déclarées dans le `:root` de `json2sql-ui/assets/styles.css`. `theme::design_css()` embarque ce fichier à la compilation (`include_str!`) : c'est la **source de vérité unique**. Un composant passe par `var(--…)` ou par une classe de ce fichier ; il n'y a pas de constante Rust de couleur.

### Surfaces (règle "No-Line")

La logique visuelle est définie par des décalages de fond, pas par des traits.

| Variable | Valeur | Rôle |
|---|---|---|
| `--bg` | `#0d1014` | Fond racine de l'application |
| `--bg-1` / `--bg-2` / `--bg-3` | `#131820` / `#1a1f29` / `#232936` | Paliers de surface, du plus sombre au plus clair (`--bg-2` : en-têtes de panneau, survol d'une ligne) |
| `--bg-elev` | `#1e242e` | Surface flottante |
| `--bg-input` | `#0e131a` | Champs de saisie, fond de la barre de progression |
| `--bg-code` | `#0a0d12` | Logs et blocs de code |

### Bordures

| Variable | Valeur | Rôle |
|---|---|---|
| `--bd` / `--bd-2` / `--bd-3` | `#262d3a` / `#323a48` / `#404a5b` | Bordures, de la plus discrète à la plus visible (`--bd` : séparation des panneaux et des tableaux) |

### Texte

| Variable | Valeur | Rôle |
|---|---|---|
| `--fg` | `#e6eaf0` | Texte principal |
| `--fg-2` | `#b2bac7` | Texte secondaire |
| `--fg-3` | `#7a8290` | Labels, métadonnées, en-têtes de tableau |
| `--fg-4` | `#555c69` | Texte effacé, commentaires SQL |

### Accent et sémantique

| Variable | Valeur | Sémantique |
|---|---|---|
| `--acc` | `#4dd0c9` | Accent principal (teal) : action, sélection |
| `--acc-hi` / `--acc-lo` | `#62e0d9` / `#2da39d` | Variantes claire et sombre (dégradé de la barre de progression) |
| `--acc-bg` / `--acc-bd` | `rgba(77, 208, 201, 0.12)` / `rgba(77, 208, 201, 0.4)` | Fond et bordure translucides (ligne sélectionnée, bouton actif) |
| `--info` | `#6aa9ff` | Information |
| `--success` | `#5bcf8e` | Succès, connexion active |
| `--warning` | `#f0b072` | Avertissement |
| `--danger` | `#e87862` | Erreur, échec |

`--info`, `--success`, `--warning` et `--danger` ont chacun une variante `-bg` (la même couleur à 12 % d'opacité) pour les fonds.

### Badges de stratégie

| Variable | Valeur | Classe | Stratégies |
|---|---|---|---|
| `--s-default` | `#6aa9ff` | `.badge.default` | `Columns` |
| `--s-jsonb` | `#b48cf0` | `.badge.jsonb` | `Jsonb` |
| `--s-flatten` | `#5bcf8e` | `.badge.flatten` | `Flatten` |
| `--s-pivot` | `#f0b072` | `.badge.pivot` | `Pivot`, `StructuredPivot`, `SiblingCollapse`, `SiblingCollapseMulti` |
| `--s-norm` | `#f0b072` | `.badge.normalize` | `AutoSplit` |
| `--s-skip` | `#e87862` | `.badge.skip` | `Skip` (IHM), tables absorbées (`Ignore`) |

La correspondance est dans `strategy_badge` et `user_override_badge` (`json2sql-ui/src/screens/common/strategy_badges.rs`).

### Rayons, ombres, densité

- Rayons : `--r-sm` (4px), `--r-md` (6px), `--r-lg` (10px), `--r-xl` (14px, défini mais pas utilisé).
- Ombres : `--shadow-1` (léger relief), `--shadow-2` (défini mais pas utilisé), `--shadow-pop` (élément flottant).
- Densité : `--row-h` (28px, hauteur de ligne), `--pad-x` (14px) et `--pad-y` (10px) pour les marges internes.

---

## 3. Typographie

| Famille | Où | Usage |
|---|---|---|
| Inter | police de `body` | Interface, labels, boutons |
| JetBrains Mono | `.mono`, blocs de code, logs | Noms de tables/colonnes, SQL, JSON, valeurs copiables |

Règle : tout texte copiable/collable dans un terminal utilise JetBrains Mono (`.mono`). Tout ce qui est interface pure utilise Inter.

Tailles : `--fs-xs` (11px), `--fs-sm` (12px), `--fs-md` (13px), `--fs-lg` (15px).

---

## 4. Élévation et profondeur

La hauteur se marque d'abord par la lumière : un palier de surface plus clair (`--bg-1` à `--bg-3`, `--bg-elev`), pas une ombre.

- **Séparation** : une bordure `--bd` de 1px entre deux panneaux.
- **Ombres** : seulement les tokens `--shadow-*` — `--shadow-1` pour un léger relief (`.card`, bouton actif d'un `.seg`), `--shadow-pop` pour un élément flottant (`.tweaks-panel`, `.proto-switcher`). L'étape active du wizard (`.step-card.active`) a un halo d'accent.
- **Ligne sélectionnée** : fond `--acc-bg` (`table.t tbody tr.sel`) ; un bouton de stratégie actif (`.strat-btn.on`) ajoute un trait d'accent à gauche (`box-shadow: inset 2px 0 0 var(--acc)`).

---

## 5. Sources CSS

| Source | Chargée par | Contenu |
|---|---|---|
| `assets/styles.css` | `theme::design_css()` → `include_str!` | Tout le design system : tokens du `:root`, layouts, composants, utilitaires, overrides webkit2gtk |

`main.rs` l'injecte dans un `<style>` du `<head>` du webview au démarrage (`with_custom_head`). C'est le seul fichier à éditer pour un nouveau composant ou un nouveau token.

---

## 6. Composants CSS — `assets/styles.css`

### Layouts

#### `.split-3`
Layout trois panneaux flex avec splitters drag-and-drop.  
Panneaux enfants : `.pane` (largeur fixe) ou `.pane.fluid` (flex:1). Dernier `.pane` sans bordure droite.  
`.pane.collapsed` → repli avec `.collapsed-strip` (bande verticale avec label rotaté).

#### `.split-60-40`
Layout deux panneaux 60/40 fixe. Utilisé par Analysis et Import.  
Panneaux : `.pane`, séparés par une bordure `--bd`.

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
Variantes : `.stat-tile.warn` (valeur orange), `.stat-tile.acc` (valeur en couleur d'accent `--acc`).

### Écrans 3 & 4 — Strategy / Preview

#### `.strat-list` / `.strat-btn`
Liste et boutons de sélection de stratégie (flex column, gap 4px).  
`.strat-btn` : fond `--bg`, bordure `--bd`, radius `--r-md`, flex row.  
`.strat-btn.on` : fond accent alpha, bordure accent — état sélectionné.  
`.strat-btn .nm` (label flex:1) + `.strat-btn .dsc` (description secondaire) + `.strat-btn .radio` (indicateur rond).

#### `table.t`
Table de données dense. `thead th` : texte xs ALL-CAPS, `--fg-3`, fond `--bg-2`, sticky.  
`tbody td` : texte xs, bordure basse `--bd`, padding `6px 10px`.  
`tr:hover` → `--bg-2` | `tr.sel` → fond `--acc-bg`.

#### `.badge`
Badge inline. Fond translucide + bordure colorée selon variante.  
Variantes sémantiques : `.badge.default`, `.badge.jsonb`, `.badge.jsonbi`, `.badge.flatten`, `.badge.normalize`, `.badge.pivot`, `.badge.skip` (voir § 2).  
Variantes état : `.badge.warn`, `.badge.danger`, `.badge.success`, `.badge.info`, `.badge.muted`, `.badge.acc`.  
Variante forme : `.badge.sq` (border-radius réduit).

### Écrans 2 & 5 — Logs

#### `.log`
Panneau de log monospace. Fond `--bg-code`, `overflow-y:auto`.  
Colorisation des lignes :  
- `.log .ts` → gris faint (timestamp)  
- `.log .warn` → orange  
- `.log .err` → rouge  
- `.log .ok` → vert  
- `.log .keyw` → bleu info  
- `.log .num` → accent

### Écrans 2 & 5 — Progress

#### `.prog`
Barre de progression. Hauteur 6px, fond `--bg-input`, dégradé d'accent `--acc-lo → --acc` (enfant `i`).  
`.prog.thick` → hauteur 10px.  
`.prog.indeterminate` → animation de balayage gauche→droite.  
`.prog.warn` → dégradé orange | `.prog.danger` → dégradé rouge.

### Écran 4 — DDL Preview

#### `pre.code` / `.code`
Bloc code monospace, fond `--bg-code`, `white-space:pre-wrap`.  
Tokens DDL SQL :  
- `.code .kw` → violet (`#b48cf0`, gras) — mots-clés SQL (`CREATE`, `TABLE`, `NOT NULL`, etc.)  
- `.code .ty` → teal (`#4dd0c9`) — types PG (`TEXT`, `INTEGER`, `JSONB`, etc.)  
- `.code .pn` → `--fg` en gras — noms de tables/colonnes  
- `.code .num` → orange — littéraux numériques  
- `.code .str` → vert — littéraux chaîne  
- `.code .com` → `--fg-4` en italique — commentaires

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

---

## 7. Règles d'application

**À faire :**
- Utiliser les paliers de surface (`--bg`, `--bg-1` à `--bg-3`) pour grouper des éléments liés sans boîtes ni lignes.
- Utiliser JetBrains Mono (`.mono`) pour tout texte qui représente de la donnée (noms de tables, types SQL, chemins).
- Aligner les labels de métadonnées à droite, les données primaires à gauche (asymétrie intentionnelle).

**À ne pas faire :**
- Ne pas dépasser `--r-lg` (10px), réservé aux cartes et panneaux (`.card`, `.step-card`, `.stat-tile`, `.tweaks-panel`) ; les contrôles restent à `--r-sm` ou `--r-md`.
- Ne pas utiliser du noir pur (`#000`) ou du blanc pur (`#FFF`). Utiliser `--bg` et `--fg`.
- Pas d'ombre en dehors des tokens `--shadow-*`. Si un élément doit ressortir, monter d'un palier de surface.
- Ne pas hardcoder de valeurs hex dans les composants Rust : passer par une variable CSS (`var(--…)`) ou une classe de `assets/styles.css`.
