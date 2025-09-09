#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Liste les valeurs output_file= ou output_path= dans les scripts Python d'un dossier.
- Gère output_file="xxx.ndjson"
- Gère output_path = DATA_DIR / "xxx.ndjson" (/ "subdir" / "yyy.ndjson")
Parcours récursif de scripts/fetch_TMDB_API.
Écrit:
  - output_files_list.txt : 1 chemin/nom par ligne (dédupliqué, trié)
  - empty_scripts.txt     : scripts sans output_file/output_path
"""

import ast
import re
import sys
from pathlib import Path

DEFAULT_DIR = Path("scripts/fetch_TMDB_API")

# Regex fallback pour les assignations directes dans le code (rare mais utile)
OUTPUT_RE = re.compile(r"(output_file|output_path)\s*=\s*([rubf]*)?['\"]([^'\"]+)['\"]", re.IGNORECASE)

def _is_div_binop(node: ast.AST) -> bool:
    return isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)

def _literal_strings_from_node(node: ast.AST):
    """
    Extrait une liste de fragments de texte issus de littéraux string dans une expression AST.
    - "foo.ndjson" -> ["foo.ndjson"]
    - DATA_DIR / "a" / "b.ndjson" -> ["a", "b.ndjson"]
    - f"{'x'}" sans expressions -> ["x"] (cas JoinedStr trivial)
    Les parties non littérales (ex: DATA_DIR) sont ignorées.
    """
    parts = []

    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        parts.append(node.value)
    elif isinstance(node, ast.Str):
        parts.append(node.s)
    elif isinstance(node, ast.JoinedStr):
        # Concatène uniquement si aucune expression non-littérale
        if all(isinstance(v, ast.Str) for v in node.values):
            parts.append("".join(v.s for v in node.values))
    elif _is_div_binop(node):
        parts.extend(_literal_strings_from_node(node.left))
        parts.extend(_literal_strings_from_node(node.right))
    # Autres nœuds: ignorer (Name, Attribute, Call, etc.)
    return [p for p in parts if p]

def extract_outputs_with_ast(py_path: Path):
    """
    Cherche:
      - appels avec kwargs output_file= / output_path=
      - assignations self.output_path = DATA_DIR / "x.ndjson" (ou similaires)
    Retourne liste de chemins/noms trouvés (fragments littéraux joints par '/').
    """
    try:
        src = py_path.read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(src, filename=str(py_path))
    except Exception:
        return []

    found = []

    # 1) Appels avec kwargs
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node, "keywords", None):
            for kw in node.keywords:
                if kw.arg in ("output_file", "output_path"):
                    frags = _literal_strings_from_node(kw.value)
                    if frags:
                        found.append("/".join(frags))

    # 2) Assignations attributaires (ex: self.output_path = DATA_DIR / "file.ndjson")
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            # Cible: self.output_path OU output_path variable directe
            targets = node.targets if isinstance(node.targets, list) else [node.targets]
            hit = False
            for t in targets:
                if isinstance(t, ast.Attribute) and t.attr in ("output_path", "output_file"):
                    hit = True
                    break
                if isinstance(t, ast.Name) and t.id in ("output_path", "output_file"):
                    hit = True
                    break
            if not hit:
                continue

            frags = _literal_strings_from_node(node.value)
            if frags:
                found.append("/".join(frags))

    return found

def extract_outputs_with_regex(py_path: Path):
    """Fallback simple pour les assignations directes en texte."""
    try:
        src = py_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []
    return [m.group(3) for m in OUTPUT_RE.finditer(src)]

def collect_outputs(root_dir: Path):
    outputs = []
    empty_scripts = []
    for py in root_dir.rglob("*.py"):
        if py.name.startswith("_") or py.name == "__init__.py":
            continue

        ast_vals = extract_outputs_with_ast(py)
        rx_vals  = extract_outputs_with_regex(py)

        vals = ast_vals + rx_vals
        if vals:
            outputs.extend(vals)
        else:
            empty_scripts.append(py)

    # Déduplique et trie
    seen = set()
    deduped = []
    for v in outputs:
        if v not in seen:
            seen.add(v)
            deduped.append(v)
    deduped.sort()
    return deduped, empty_scripts

def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DIR
    if not root.exists():
        raise SystemExit(f"Dossier introuvable: {root}")

    results, empty_scripts = collect_outputs(root)

    Path("output_files_list.txt").write_text(
        "\n".join(results) + ("\n" if results else ""), encoding="utf-8"
    )
    Path("empty_scripts.txt").write_text(
        "\n".join(map(str, empty_scripts)) + ("\n" if empty_scripts else ""), encoding="utf-8"
    )

    print("=== Outputs détectés (file/path) ===")
    for v in results:
        print(v)

    if empty_scripts:
        print("\n=== Scripts sans output_file/output_path détecté ===")
        for s in empty_scripts:
            print(s)

if __name__ == "__main__":
    main()
