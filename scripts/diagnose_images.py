#!/usr/bin/env python3
"""
Diagnostic JSONL par seek aléatoire — ne lit PAS tout le fichier.
Saute à N offsets aléatoires dans le fichier, lit la ligne complète à chaque offset.
Résultat : analyse de N lignes représentatives en quelques secondes même sur 70GB.

Usage:
    python3 scripts/diagnose_images.py --input /path/to/file.jsonl --output /tmp/report.txt
    python3 scripts/diagnose_images.py --input /path/to/file.jsonl --key images.uploaded --samples 5000
"""

import json
import os
import random
import sys
import argparse
from collections import Counter
from pathlib import Path


def get_nested(obj, path: str):
    parts = path.split(".")
    cur = obj
    for p in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def sample_lines(path: Path, n: int, seed: int = 42) -> list[bytes]:
    """Lit n lignes à des offsets aléatoires dans le fichier (sans tout charger)."""
    file_size = os.path.getsize(path)
    rng = random.Random(seed)
    offsets = sorted(rng.sample(range(file_size - 1), min(n * 2, file_size - 1)))

    lines = []
    seen_offsets = set()

    with open(path, "rb") as f:
        for offset in offsets:
            if len(lines) >= n:
                break
            f.seek(offset)
            f.readline()          # ligne partielle → skip
            line_start = f.tell()
            if line_start in seen_offsets:
                continue
            line = f.readline()
            if not line or not line.strip():
                continue
            seen_offsets.add(line_start)
            lines.append(line)

    return lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",   "-i", required=True)
    parser.add_argument("--key",     "-k", default="images.uploaded",
                        help="Chemin JSON à analyser (défaut: images.uploaded)")
    parser.add_argument("--samples", "-s", type=int, default=3000,
                        help="Nombre de lignes à échantillonner (défaut: 3000)")
    parser.add_argument("--output",  "-o", default=None,
                        help="Fichier de sortie (défaut: stdout)")
    parser.add_argument("--seed",          type=int, default=42)
    parser.add_argument("--show-examples", type=int, default=5)
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Erreur: fichier introuvable: {input_path}", file=sys.stderr)
        sys.exit(1)

    out_path = Path(args.output) if args.output else None
    out = open(out_path, "w", encoding="utf-8") if out_path else sys.stdout

    file_size_gb = os.path.getsize(input_path) / 1e9
    print(f"Fichier : {input_path.name} ({file_size_gb:.1f} GB)", file=sys.stderr)
    print(f"Seek aléatoire : {args.samples} lignes échantillonnées ...", file=sys.stderr)

    raw_lines = sample_lines(input_path, args.samples, seed=args.seed)
    print(f"Lignes lues    : {len(raw_lines)}", file=sys.stderr)
    if out_path:
        print(f"Sortie         : {out_path}", file=sys.stderr)
    print(file=sys.stderr)

    # ── Analyse ──────────────────────────────────────────────────────────────
    parsed = 0
    has_key = 0
    missing_key = 0

    child_key_types = Counter()
    child_count_dist = Counter()

    numeric_child_fields = Counter()
    numeric_child_has_obj_children = 0
    numeric_child_pure_containers = 0

    sizes_key_patterns = Counter()
    sizes_key_types = Counter()
    sizes_child_fields = Counter()

    pure_container_examples = []
    examples_with_data = []

    for raw in raw_lines:
        try:
            record = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            continue
        parsed += 1

        target = get_nested(record, args.key)
        if target is None:
            missing_key += 1
            continue

        has_key += 1

        if not isinstance(target, dict) or not target:
            child_key_types["empty_or_non_object"] += 1
            continue

        keys = list(target.keys())
        num_keys = [k for k in keys if k.isdigit()]
        txt_keys = [k for k in keys if not k.isdigit()]
        child_count_dist[len(keys)] += 1

        if num_keys and txt_keys:
            child_key_types["mixed"] += 1
        elif num_keys:
            child_key_types["all_numeric"] += 1
        else:
            child_key_types["all_text"] += 1

        for nk in num_keys[:5]:
            child_obj = target[nk]
            if not isinstance(child_obj, dict):
                continue

            scalar_fields = []
            obj_children = []
            for field, val in child_obj.items():
                if isinstance(val, dict):
                    obj_children.append(field)
                elif not isinstance(val, list):
                    scalar_fields.append(field)

            for sf in scalar_fields:
                numeric_child_fields[sf] += 1

            if obj_children:
                numeric_child_has_obj_children += 1

            if not scalar_fields:
                numeric_child_pure_containers += 1
                if len(pure_container_examples) < args.show_examples:
                    pure_container_examples.append({
                        "path": f"{args.key}.{nk}",
                        "obj_children": obj_children,
                        "structure": {k: type(v).__name__ for k, v in child_obj.items()},
                    })
            elif len(examples_with_data) < args.show_examples:
                examples_with_data.append({
                    "path": f"{args.key}.{nk}",
                    "scalaires": scalar_fields,
                    "obj_children": obj_children,
                })

            if "sizes" in obj_children:
                sizes_obj = child_obj.get("sizes", {})
                if isinstance(sizes_obj, dict):
                    for sk, sv in sizes_obj.items():
                        sizes_key_patterns[sk] += 1
                        sizes_key_types["numeric" if sk.isdigit() else "text"] += 1
                        if isinstance(sv, dict):
                            for sf2 in sv.keys():
                                sizes_child_fields[sf2] += 1

    # ── Rapport ──────────────────────────────────────────────────────────────
    def p(*a, **kw):
        print(*a, **kw, file=out)

    p(f"=== DIAGNOSTIC '{args.key}' ===")
    p(f"Fichier  : {input_path.name} ({file_size_gb:.1f} GB)")
    p(f"Lignes JSON parsées : {parsed}")
    p(f"  → avec '{args.key}'  : {has_key}")
    p(f"  → sans '{args.key}'  : {missing_key}")
    p()

    p(f"--- Type de clés sous '{args.key}' ---")
    for k, v in child_key_types.most_common():
        p(f"  {k}: {v}")
    p()

    p(f"--- Distribution du nombre d'enfants directs ---")
    for count in sorted(child_count_dist.keys())[:25]:
        p(f"  {count:4d} enfants : {child_count_dist[count]} fois")
    if len(child_count_dist) > 25:
        p(f"  ... ({len(child_count_dist)} valeurs distinctes au total)")
    p()

    if numeric_child_fields:
        p(f"--- Champs scalaires dans {args.key}.<N> ---")
        for field, cnt in numeric_child_fields.most_common(20):
            p(f"  '{field}': {cnt} occurrences")
        p()

    p(f"--- Pureté des enfants numériques ---")
    p(f"  Avec objets enfants (ex: sizes) : {numeric_child_has_obj_children}")
    p(f"  PURS CONTAINERS (0 scalaire)    : {numeric_child_pure_containers}  ← si > 0 : BUG JACCARD")
    p()

    if pure_container_examples:
        p(f"--- Exemples de pure containers ---")
        for ex in pure_container_examples:
            p(f"  {ex['path']}")
            p(f"    enfants objet : {ex['obj_children']}")
            p(f"    structure     : {ex['structure']}")
        p()

    if examples_with_data:
        p(f"--- Exemples d'entrées avec données scalaires ---")
        for ex in examples_with_data:
            p(f"  {ex['path']}")
            p(f"    scalaires     : {ex['scalaires']}")
            p(f"    enfants objet : {ex['obj_children']}")
        p()

    if sizes_key_patterns:
        p(f"--- Clés dans .sizes ---")
        for sk, cnt in sizes_key_patterns.most_common(15):
            tag = "numeric" if sk.isdigit() else "text"
            p(f"  '{sk}' ({tag}): {cnt}")
        p()
        p(f"--- Répartition numeric/text dans .sizes ---")
        for t, cnt in sizes_key_types.most_common():
            p(f"  {t}: {cnt}")
        p()
        if sizes_child_fields:
            p(f"--- Champs dans sizes.<clé> ---")
            for f, cnt in sizes_child_fields.most_common(10):
                p(f"  '{f}': {cnt}")
            p()

    p("=== FIN ===")

    if out is not sys.stdout:
        out.close()
        print(f"Résultats → {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
