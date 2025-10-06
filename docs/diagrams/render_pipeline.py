"""
Pipeline Diagram with Embedded Icons
====================================

This script builds a complex Snowflake/ML pipeline diagram using Graphviz
and then post‑processes the generated SVG to inline all referenced images
as data URIs.  In the Graphviz documentation it is clear that when
generating SVG output, images referenced via the ``image`` attribute or
HTML ``<IMG>`` tags are **not** embedded into the output.  Only the
filename is written into the SVG, so the viewer must have access to
those files.  The FAQ notes that when using ``-Tsvg`` the image's
contents are *not* copied into the SVG, only the file name; therefore
the file must be available to the viewer【870924154717203†L738-L741】.  To make
the SVG self‑contained and portable, we use a small helper to replace
all external image references with base64‑encoded data URIs.

Run this script from the project root.  It will generate
``pipeline_graphviz_fixed.svg`` in the same directory as the script.
"""

from __future__ import annotations

import base64
import subprocess
import shlex
import xml.etree.ElementTree as ET
from pathlib import Path
from graphviz import Digraph

# =========================
# Style global (dark-friendly, slide-ready)
# =========================
FONT = "DejaVu Sans"
EDGE = "#BFBFBF"

# Directory containing the original SVG icons.  Icons are stored in
# ``<project>/icons`` with each icon as a vector graphic.  Converted
# PNGs are cached in ``<project>/icons/_png``.  See the ``image``
# attribute docs for why local file paths are required【107958119370961†L350-L356】.
ICON_DIR = (Path(__file__).resolve().parent.parent / "icons").resolve()
PNG_DIR = (ICON_DIR / "_png").resolve()
PNG_DIR.mkdir(parents=True, exist_ok=True)

COLOR: dict[str, str] = {
    "dev":        "#1f2937",  # slate-800
    "ingest":     "#0b3a53",  # teal-900 (stages/put/snowpipe)
    "raw":        "#334155",  # slate-700
    "bronze":     "#b45309",  # amber-700
    "silver":     "#6b7280",  # gray-500
    "gold":       "#a16207",  # yellow-700
    "api":        "#065f46",  # emerald-800
    "ml":         "#3b82f6",  # blue-500
    "orches":     "#7c3aed",  # violet-600
    "ci":         "#111827",  # gray-900
    "mon":        "#0f172a",  # slate-900
}


def ensure_png(icon_name: str, size: int = 56) -> Path | None:
    """
    Convert a vector icon (SVG) from ``ICON_DIR`` to a PNG of the given size.
    Cached results are stored under ``_png/``.  If the input is already a
    PNG it is returned unchanged.  Returns ``None`` if the source file
    cannot be found or the conversion fails.
    """
    src = ICON_DIR / icon_name
    if not src.exists():
        return None
    # If user already provided a PNG, return as is.
    if src.suffix.lower() == ".png":
        return src
    # Destination path in the PNG cache.
    dst = PNG_DIR / (src.stem + ".png")
    if dst.exists():
        return dst
    # Convert the SVG to PNG via rsvg-convert.  This requires
    # librsvg2-bin on the system.  Use shell quoting to avoid injection.
    try:
        cmd = f'rsvg-convert -w {size} -h {size} -o {shlex.quote(str(dst))} {shlex.quote(str(src))}'
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if dst.exists():
            return dst
    except Exception:
        return None
    return None


def html_label(title: str, subtitle: str = "", icon: str | None = None, size: int = 56) -> str:
    """
    Build an HTML‑like label for a Graphviz node.  The resulting string is
    wrapped in ``<`` and ``>`` so Graphviz treats it as HTML.  Each label
    contains an optional image row and two text rows.  Note that the
    ``<IMG>`` element's ``SRC`` attribute refers to a local file path; the
    image will not be inlined in the SVG output【870924154717203†L736-L741】.  After
    rendering, ``embed_images_in_svg`` will replace these external
    references with data URIs.
    """
    img_row = ""
    if icon:
        p = ensure_png(icon, size=size)
        if p and p.exists():
            # Set explicit width/height so viewers know the icon dimensions.
            img_row = f'<TR><TD><IMG SRC="{p}" WIDTH="{size}" HEIGHT="{size}"/></TD></TR>'
    # Replace newline characters with HTML <BR/> tags.
    title_html = title.replace("\n", "<BR/>")
    subtitle_html = subtitle.replace("\n", "<BR/>") if subtitle else ""
    subrow = f'<TR><TD><FONT POINT-SIZE="10">{subtitle_html}</FONT></TD></TR>' if subtitle else ""
    # Wrap the content in a TABLE.  CELLBORDER=0 removes inner borders.
    return (
        '<'
        '\n<TABLE BORDER="0" CELLBORDER="0" CELLPADDING="2">'
        f'\n  {img_row}'
        f'\n  <TR><TD><B>{title_html}</B></TD></TR>'
        f'\n  {subrow}'
        '\n</TABLE>\n>'
    )


def node_box(g: Digraph, name: str, title: str, subtitle: str = "", icon: str | None = None, size: int = 52) -> None:
    """Add a node with a nicely formatted label and optional icon."""
    g.node(name, label=html_label(title, subtitle, icon, size=size))


def embed_images_in_svg(svg_path: str | Path) -> None:
    """
    Post‑process an SVG produced by Graphviz, replacing all external image
    references with inline base64 encoded data URIs.  Graphviz does not
    embed images in SVG output【870924154717203†L736-L741】, so we must fix this
    ourselves.  Supports PNG, JPEG and GIF images.  The SVG is modified
    in place.

    Parameters
    ----------
    svg_path : str or Path
        Path to the generated SVG file.
    """
    svg_file = Path(svg_path)
    if not svg_file.exists():
        return
    # Register XML namespaces used by Graphviz (svg and xlink).
    ET.register_namespace('', "http://www.w3.org/2000/svg")
    ET.register_namespace('xlink', "http://www.w3.org/1999/xlink")
    tree = ET.parse(svg_file)
    root = tree.getroot()
    changed = False
    # Graphviz writes images as <image> elements with xlink:href attributes.
    for image in root.findall('.//{http://www.w3.org/2000/svg}image'):
        href = image.get('{http://www.w3.org/1999/xlink}href')
        if not href or href.startswith('data:'):
            continue  # Already embedded
        # Resolve the path relative to the SVG file's directory.
        img_path = (svg_file.parent / href).resolve()
        if not img_path.exists():
            # Fall back to search in ICON_DIR if relative path fails.
            candidate = ICON_DIR / Path(href).name
            img_path = candidate if candidate.exists() else None
        if img_path and img_path.exists():
            ext = img_path.suffix.lower().lstrip('.')
            mime = {
                'png': 'image/png',
                'jpg': 'image/jpeg',
                'jpeg': 'image/jpeg',
                'gif': 'image/gif',
                'svg': 'image/svg+xml',
            }.get(ext, 'application/octet-stream')
            data = img_path.read_bytes()
            b64 = base64.b64encode(data).decode('ascii')
            image.set('{http://www.w3.org/1999/xlink}href', f'data:{mime};base64,{b64}')
            changed = True
    if changed:
        tree.write(svg_file, encoding='utf-8')


def build_pipeline() -> str:
    """
    Construct the pipeline diagram using Graphviz and return the path to
    the generated SVG file.  After rendering the graph, all icon
    references are embedded into the SVG via `embed_images_in_svg`.

    Returns
    -------
    str
        The absolute path to the final SVG file with embedded images.
    """
    g = Digraph("absolute_cinema_pipeline", filename="pipeline_graphviz_fixed", format="svg")
    g.attr(bgcolor="transparent", rankdir="LR", ranksep="0.9", nodesep="0.9", splines="spline")
    g.attr("node", shape="plain", fontname=FONT)
    g.attr("edge", color=EDGE, penwidth="1.6", arrowsize="0.8", fontname=FONT)

    # Cluster DEV (VS Code / scripts / sources)
    with g.subgraph(name="cluster_dev") as dev:
        dev.attr(label="DEV — VS Code & Sources", color=COLOR["dev"], fontcolor="black", style="rounded", penwidth="1.2", fontname=FONT)
        node_box(dev, "vscode", "VS Code", icon="vscode.svg")
        node_box(dev, "onedrive", "OneDrive", "JSONs dumps", icon="onedrive.svg")
        node_box(dev, "tmdb_dumps", "TMDB Dumps", "movie_ids.json.gz\n tv_series_ids.json.gz", icon="tmdb.svg")
        node_box(dev, "scripts", "Scripts", "fetch_dumps_daily.sh\nmerge_tmdb_into_final.py", icon="bash.svg")
        node_box(dev, "tmdb_api", "TMDB API", "enrichments (details, credits,\nproviders, genres…)", icon="tmdb.svg")
        node_box(dev, "upload_py", "upload_to_stage.py", "PUT to Snowflake stages", icon="python.svg")
        node_box(dev, "repo", "Repo GitHub", "", icon="github.svg")
        dev.edge("vscode", "scripts", style="dashed")
        dev.edge("tmdb_dumps", "scripts", label="input", style="dotted")
        dev.edge("scripts", "tmdb_api", label="requests", style="dotted")
        dev.edge("scripts", "onedrive", label="JSONL out", style="dotted")
        dev.edge("onedrive", "upload_py", label="file paths", style="dotted")
        dev.edge("vscode", "repo", style="invis")

    # Cluster Snowflake — Ingestion → RAW → BRONZE → SILVER → GOLD
    with g.subgraph(name="cluster_snowflake") as sf:
        sf.attr(label="Snowflake — Ingestion & Modèle Analytique", color="#60a5fa", fontcolor="black", style="rounded", penwidth="1.2", fontname=FONT)
        node_box(sf, "stages", "Stages", "Stockage JSONs", icon="stage.svg")
        node_box(sf, "snowpipe", "Snowpipe", "auto-ingest", icon="snowpipe.svg")
        node_box(sf, "raw", "RAW", "Tables VARIANT", icon="table_raw.svg")
        node_box(sf, "bronze", "BRONZE", "parse + flatten\nDynamic Tables", icon="bronze.svg")
        node_box(sf, "silver", "SILVER", "dédoublonnage\nDynamic Tables", icon="silver.svg")
        node_box(sf, "gold", "GOLD", "modèle analytique\nFCT_CONTENT + DIM/BRIDGE", icon="gold.svg")
        node_box(sf, "warehouse_api", "Warehouse API", "SNOWFLAKE_WAREHOUSE_API", icon="snowflake.svg")
        sf.edge("stages", "snowpipe", label="NEW file")
        sf.edge("snowpipe", "raw", label="COPY INTO")
        sf.edge("raw", "bronze")
        sf.edge("bronze", "silver")
        sf.edge("silver", "gold")
        sf.edge("warehouse_api", "gold", style="invis")

    # Connect DEV → Snowflake
    g.edge("upload_py", "stages", label="PUT (auto_compress)")
    g.edge("tmdb_api", "scripts", style="invis")

    # Cluster ML (features + modèle)
    with g.subgraph(name="cluster_ml") as ml:
        ml.attr(label="ML — Features & Modèle", color=COLOR["ml"], fontcolor="black", style="rounded", penwidth="1.2", fontname=FONT)
        node_box(ml, "features_tbl", "GLD_FACT_MOVIE_ML_NUMERIC", "features d'entraînement", icon="table.svg")
        node_box(ml, "sklearn", "scikit-learn", "DecisionTreeRegressor\nimputer.pkl / features.pkl\nsaved_model.pkl", icon="sklearn.svg")
        ml.edge("features_tbl", "sklearn", label="train / eval")

    # GOLD → features
    g.edge("gold", "features_tbl", label="SELECT features")

    # Cluster API & Apps (FastAPI, Uvicorn, Streamlit)
    with g.subgraph(name="cluster_api") as api:
        api.attr(label="API & Apps", color=COLOR["api"], fontcolor="black", style="rounded", penwidth="1.2", fontname=FONT)
        node_box(api, "fastapi", "FastAPI", "/v1/content /person /\nstats /reviews /finance\n/health /predict", icon="fastapi.svg")
        node_box(api, "uvicorn", "Uvicorn", "ASGI server", icon="uvicorn.svg")
        node_box(api, "streamlit", "Streamlit", "app/app.py", icon="streamlit.svg")
        node_box(api, "docker_app", "Docker", "service: app / upload /\nfull_update", icon="docker.svg")
        api.edge("uvicorn", "fastapi", dir="back", arrowhead="none", style="dotted")
        api.edge("docker_app", "fastapi", style="dotted", label="container")

    # API ↔ GOLD/ML
    g.edge("fastapi", "gold", label="SQL (connector)", dir="both")
    g.edge("fastapi", "sklearn", label="predict()", dir="both")

    # Cluster Orchestration (Airflow / Cron / Systemd)
    with g.subgraph(name="cluster_orch") as orch:
        orch.attr(label="Orchestration", color=COLOR["orches"], fontcolor="black", style="rounded", penwidth="1.2", fontname=FONT)
        node_box(orch, "airflow", "Airflow", "DAG: daily_full_update", icon="airflow.svg")
        node_box(orch, "cron", "cron", "09:15 UTC", icon="cron.svg")
        node_box(orch, "systemd", "systemd", "full_update.service", icon="systemd.svg")
        orch.edge("airflow", "docker_app", label="run full_update", style="dotted")
        orch.edge("cron", "docker_app", label="run full_update", style="dotted")
        orch.edge("systemd", "docker_app", label="run full_update", style="dotted")

    # Cluster CI/CD (GitHub Actions)
    with g.subgraph(name="cluster_ci") as ci:
        ci.attr(label="CI/CD — GitHub Actions", color=COLOR["ci"], fontcolor="black", style="rounded", penwidth="1.2", fontname=FONT)
        node_box(ci, "gh_actions", "Workflow CI", ".github/workflows/ci.yml\nbuild Docker (no tests)", icon="github_actions.svg")
        ci.edge("repo", "gh_actions", label="push / PR")

    # Cluster Monitoring (Prometheus/Grafana/cAdvisor/Blackbox)
    with g.subgraph(name="cluster_mon") as mon:
        mon.attr(label="Monitoring — Prometheus & Grafana", color=COLOR["mon"], fontcolor="black", style="rounded", penwidth="1.2", fontname=FONT)
        node_box(mon, "prometheus", "Prometheus", "monitoring/prometheus.yml", icon="prometheus.svg")
        node_box(mon, "grafana", "Grafana", "dashboards: cAdvisor + API", icon="grafana.svg")
        node_box(mon, "cadvisor", "cAdvisor", "metrics système conteneurs", icon="cadvisor.svg")
        node_box(mon, "blackbox", "Blackbox Exporter", "probe /health", icon="blackbox_exporter.svg")
        node_box(mon, "mon_net", "mon_net", "réseau monitoring", icon="network.svg")
        mon.edge("prometheus", "cadvisor", label="scrape :8080/metrics", dir="both")
        mon.edge("prometheus", "blackbox", label="scrape /probe?target=API", dir="both")
        mon.edge("grafana", "prometheus", label="datasource", dir="both")
        mon.edge("blackbox", "fastapi", label="HTTP probe /health", style="dotted")
        mon.edge("cadvisor", "docker_app", style="dotted", label="Docker metrics")

    # Render the graph.  This writes an SVG file under the current directory.
    svg_path = g.render(directory=str(Path(__file__).parent), cleanup=True)
    # Inline all images so the diagram is self‑contained.
    embed_images_in_svg(svg_path)
    return svg_path


if __name__ == "__main__":
    final_svg = build_pipeline()
    print("Generated SVG at", final_svg)