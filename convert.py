#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import sys
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict

def fetch_bytes(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (XML2CSV bot)"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()

def detect_items(root: ET.Element):
    """
    Znajdź rodzica, który ma najwięcej dzieci o tym samym tagu (np. <product>..).
    Zwraca listę elementów-itemów oraz nazwę tagu.
    """
    best = (0, None, None)  # (count, parent, tag)
    for parent in root.iter():
        counts = {}
        for child in list(parent):
            counts[child.tag] = counts.get(child.tag, 0) + 1
        for tag, cnt in counts.items():
            if cnt > best[0]:
                best = (cnt, parent, tag)
    if best[2] is None:
        # fallback: weź bezpośrednie dzieci root
        return list(root), root.tag
    parent, tag = best[1], best[2]
    items = [c for c in list(parent) if c.tag == tag]
    return items, tag

def iter_leaves(elem: ET.Element, prefix=""):
    """
    Zwracaj krotki (key, value) dla wszystkich liści (tekstowych) w elemencie (rekurencyjnie).
    Dodaje też atrybuty jako klucze postaci <tag>@attr.
    """
    # Atrybuty bieżącego elementu
    for attr, val in elem.attrib.items():
        key = f"{prefix}{elem.tag}@{attr}"
        if val is not None and str(val).strip():
            yield (key, str(val).strip())

    children = list(elem)
    text = (elem.text or "").strip() if elem.text else ""
    if not children:
        # liść tekstowy
        if text:
            key = f"{prefix}{elem.tag}"
            yield (key, text)
        return

    # ma dzieci -> schodzimy w dół
    for child in children:
        new_prefix = prefix
        # jeśli chcemy spłaszczyć nazwę, użyjemy "tag_" jako separatora
        if elem.tag:
            new_prefix = f"{prefix}{elem.tag}_"
        yield from iter_leaves(child, new_prefix)

def flatten_item(elem: ET.Element) -> dict:
    """
    Spłaszcza element do słownika: duplicate keys -> łączone ' | '.
    """
    bucket = defaultdict(list)
    for k, v in iter_leaves(elem, prefix=""):
        bucket[k].append(v)
    # scalenie wartości
    flat = {}
    for k, vals in bucket.items():
        # usuń duplikaty zachowując kolejność
        seen = set()
        uniq = []
        for x in vals:
            if x not in seen:
                uniq.append(x)
                seen.add(x)
        # pola wielowartościowe łączymy separatorem ' | ' (dobry pod obrazki)
        flat[k] = " | ".join(uniq)
    return flat

def write_csv(rows, out_path: str):
    if not rows:
        # brak danych – i tak utwórz pusty CSV z BOM i 1 kolumną dla czytelności
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["no_data"])
        return

    # nagłówki = suma kluczy ze wszystkich wierszy
    headers = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                headers.append(k)
                seen.add(k)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=",", quotechar='"')
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in headers})

def main():
    parser = argparse.ArgumentParser(description="Convert XML feed to CSV (auto schema).")
    parser.add_argument("--url", required=False, default=os.environ.get("SOURCE_URL", ""), help="Źródłowy URL XML")
    parser.add_argument("--out", required=False, default="data/konopnysklep.csv", help="Ścieżka wyjściowego CSV")
    parser.add_argument("--force-tag", required=False, default=os.environ.get("ITEM_TAG", ""), help="Wymuś nazwę tagu itemów (np. product/item/offer)")
    args = parser.parse_args()

    if not args.url:
        print("ERROR: Brak URL (podaj --url lub ustaw SECRET SOURCE_URL).", file=sys.stderr)
        sys.exit(1)

    xml_bytes = fetch_bytes(args.url)
    # bezpieczne parsowanie
    root = ET.fromstring(xml_bytes)

    if args.force_tag:
        # ręczne wymuszenie (jeśli znasz tag powtarzalny)
        items = [e for e in root.iter() if e.tag == args.force_tag]
        if not items:
            print(f"UWAGA: Nie znaleziono tagu '{args.force_tag}', wykrywam automatycznie...", file=sys.stderr)
            items, _ = detect_items(root)
    else:
        items, _ = detect_items(root)

    rows = [flatten_item(e) for e in items]
    write_csv(rows, args.out)
    print(f"OK: zapisano {len(rows)} rekordów do {args.out}")

if __name__ == "__main__":
    main()
