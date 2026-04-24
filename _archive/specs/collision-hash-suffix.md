# Spec : Gestion des collisions de sanitisation par hash suffix

## Problème

Plusieurs clés JSON distinctes peuvent produire le même identifiant PostgreSQL après sanitisation.
Exemple OpenFoodFacts : `ja:カルシウム`, `ja:脂質`, `ja:たんぱく質` → toutes sanitisées en `ja`.

Actuellement, `NamingRegistry::column_name()` est stateless — il ne détecte pas les collisions.

---

## Solution : hash suffix automatique

Quand deux clés originales différentes produisent le même nom sanitisé, ajouter un suffixe `_<7hex>` dérivé du nom original pour garantir l'unicité.

### Comportement attendu

```
ja:カルシウム  →  ja_3a2f1bc
ja:脂質        →  ja_9c4e012
ja:たんぱく質  →  ja_f1a8d43
calcium        →  calcium          ← pas de collision, pas de hash
```

### Règle de déclenchement

- Si `sanitize_identifier(key_a) == sanitize_identifier(key_b)` pour deux clés différentes → les deux reçoivent un hash suffix
- Si une seule clé produit ce nom sanitisé → pas de hash (comportement actuel inchangé)

---

## Implémentation

### Changement dans `NamingRegistry`

Ajouter une méthode `column_name_registry` qui maintient un état de collision (comme `table_name()` pour les tables) :

```rust
/// State pour la détection de collisions de noms de colonnes.
/// Keyed by sanitized name → liste des original_names qui y ont mené.
pub struct ColumnNameRegistry {
    // sanitized_name → [original_name, ...]
    collisions: HashMap<String, Vec<String>>,
}

impl ColumnNameRegistry {
    /// Enregistre toutes les colonnes d'une table, détecte les collisions.
    pub fn register_columns(&mut self, original_names: &[&str]);

    /// Retourne le nom final (avec hash suffix si collision).
    pub fn resolve(&self, original_name: &str) -> String;
}
```

### Calcul du hash suffix

```rust
fn column_hash_suffix(original: &str) -> String {
    // FNV-1a ou xxHash du nom original → 7 hex chars
    // Même algorithme que truncate_to_pg_limit()
    format!("{:07x}", fnv1a_hash(original) & 0xFFF_FFFF)
}
```

### Intégration dans `registry.rs` `finalize()`

Dans la boucle de construction des colonnes, remplacer l'appel stateless :
```rust
// Avant
let col_name = NamingRegistry::column_name(original_field);

// Après
let col_name = col_name_registry.resolve(original_field);
```

Le `ColumnNameRegistry` est construit en deux phases :
1. Pré-scan : enregistrer tous les `original_field` de la `TableEntry`
2. Résolution : pour chaque champ, `resolve()` retourne le nom final

---

## Comportement utilisateur

### Warning affiché après passe 1

```
WARNING: 47 column name collision(s) in table 'ingredient_nutriments':
  'ja:カルシウム', 'ja:脂質', 'ja:たんぱく質', ... → all sanitized to 'ja'
  Resolved as: ja_3a2f1bc, ja_9c4e012, ja_f1a8d43, ...
```

### Dans --schema-report

Les colonnes avec hash suffix sont annotées `[collision resolved]` pour que l'utilisateur comprenne l'origine.

### Override TOML

L'utilisateur peut renommer via schema-config en utilisant le nom résolu :
```toml
[ingredient_nutriments]
ja_3a2f1bc = "TEXT"   # override du type de la colonne résolue
```

---

## Limites

- Les noms de colonnes deviennent illisibles pour les clés non-ASCII → à combiner avec la stratégie Pivot ou Jsonb sur ces tables pour éviter que ces colonnes apparaissent en SQL
- Ne résout pas le problème de fond (clés sémantiquement identiques en plusieurs langues) — c'est l'objet de la spec `structured-pivot`

---

## Priorité et effort

- **Effort :** Faible (~50 lignes, modification de `NamingRegistry` uniquement)
- **Impact :** Bloquant pour toute import OpenFoodFacts sans stratégie Pivot/Jsonb
- **Dépendance :** Aucune — peut être implémentée indépendamment de structured-pivot
