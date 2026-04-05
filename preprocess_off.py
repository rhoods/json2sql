#!/usr/bin/env python3
"""
Pré-traitement du fichier OpenFoodFacts JSONL.
Supprime les clés dynamiques qui explosent le schéma :
  - *_ocr_<timestamp> et *_ocr_<timestamp>_result
  - *_debug_tags (clés techniques internes)
  - images.uploaded / images.selected (données techniques de taille d'image)
Sérialise en JSON string les champs à haute cardinalité :
  - nutriments → TEXT (interrogeable via -> en PostgreSQL)
"""

import json
import re
import os

OCR_PATTERN   = re.compile(r'_ocr_\d+(_result)?$')
DEBUG_PATTERN = re.compile(r'_debug_tags$')

INPUT  = "/media/dylan/data/openfoodfacts-products.jsonl"
OUTPUT = "/media/dylan/data/openfoodfacts-products-clean.jsonl"


def clean_record(d: dict) -> dict:
    cleaned = {}
    for k, v in d.items():
        # Supprimer clés OCR avec timestamp
        if OCR_PATTERN.search(k):
            continue
        # Supprimer clés debug techniques
        if DEBUG_PATTERN.search(k):
            continue
        # Sérialiser nutriments en JSON string pour éviter l'explosion de colonnes
        if k == "nutriments" and isinstance(v, dict):
            cleaned[k] = json.dumps(v, ensure_ascii=False)
            continue
        cleaned[k] = v

    # Supprimer images.uploaded et images.selected (données techniques)
    if "images" in cleaned:
        imgs = cleaned["images"]
        if isinstance(imgs, dict):
            imgs.pop("uploaded", None)
            imgs.pop("selected", None)
            if not imgs:
                del cleaned["images"]

    return cleaned


def main():
    total = 0
    errors = 0
    input_size = os.path.getsize(INPUT)

    with open(INPUT, "r", encoding="utf-8") as fin, \
         open(OUTPUT, "w", encoding="utf-8") as fout:

        for line in fin:
            total += 1
            try:
                d = json.loads(line)
                d = clean_record(d)
                fout.write(json.dumps(d, ensure_ascii=False))
                fout.write("\n")
            except json.JSONDecodeError:
                errors += 1
                continue

            if total % 100_000 == 0:
                print(f"  {total:,} lignes traitées...", flush=True)

    print(f"\nTerminé : {total:,} lignes, {errors} erreurs JSON ignorées")
    print(f"Fichier de sortie : {OUTPUT}")


if __name__ == "__main__":
    main()
