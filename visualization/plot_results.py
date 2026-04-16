"""
Skrypt wizualizacji wyników benchmarku PostgreSQL.

Generuje wykresy PNG gotowe do wklejenia w sprawozdaniu.

Wykresy:
  1. INSERT – avg_seconds per scenario (bar chart)
  2. INSERT – ops_per_sec per scenario (bar chart)
  3. INSERT – porównanie 3 skal (grouped bar)
  4. READ   – porównanie przed/po indeksach (grouped bar, avg_seconds)
  5. UPDATE – porównanie przed/po indeksach (grouped bar, avg_seconds)
  6. DELETE – porównanie przed/po indeksach (grouped bar, avg_seconds)
  7. Zbiorcze: wszystkie 24 scenariusze (horizontal bar – avg_seconds, with_indexes)
  8. Speedup indeksów: ile razy szybciej z indeksem (READ + UPDATE + DELETE)

Użycie:
  pip install pandas matplotlib seaborn
  python plot_results.py --results-dir ../postgres/results --output-dir ./charts

Flagi:
  --results-dir  katalog z plikami CSV (domyślnie: ../postgres/results)
  --output-dir   katalog wyjściowy dla PNG (domyślnie: ./charts)
  --dpi          rozdzielczość wykresów (domyślnie: 150)
  --style        styl matplotlib: seaborn-v0_8-whitegrid | ggplot | bmh (domyślnie: seaborn-v0_8-whitegrid)
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional, Dict, List

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import seaborn as sns


# ---------------------------------------------------------------------------
# PALETTE
# ---------------------------------------------------------------------------

COLOR_NO_IDX  = "#e07070"   # czerwony – bez indeksów
COLOR_WITH_IDX = "#5ba85b"  # zielony  – z indeksami

CRUD_COLORS = {
    "INSERT": "#4c78a8",
    "READ":   "#54a24b",
    "UPDATE": "#f58518",
    "DELETE": "#e45756",
}

SCALE_PALETTE = ["#4c78a8", "#72b7b2", "#ff9da6"]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        print(f"  [POMINIĘTO] brak pliku: {path}")
        return None
    df = pd.read_csv(path)
    print(f"  [OK] wczytano {len(df)} wierszy z {path.name}")
    return df


def savefig(fig: plt.Figure, out_dir: Path, name: str, dpi: int) -> None:
    out_path = out_dir / name
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    print(f"  -> zapisano: {out_path}")
    plt.close(fig)


def _rotate_labels(ax: plt.Axes, angle: int = 25) -> None:
    ax.set_xticklabels(ax.get_xticklabels(), rotation=angle, ha="right", fontsize=9)


# ---------------------------------------------------------------------------
# PLOT 1 & 2: INSERT – avg time and ops/sec per scenario (3 runs averaged)
# ---------------------------------------------------------------------------

def plot_insert_time_per_scenario(df: pd.DataFrame, out_dir: Path, dpi: int) -> None:
    agg = df.groupby(["scale", "scenario"])["seconds"].mean().reset_index()
    agg.rename(columns={"seconds": "avg_seconds"}, inplace=True)

    scales = sorted(agg["scale"].unique())
    scenarios = sorted(agg["scenario"].unique())
    colors = dict(zip(scales, SCALE_PALETTE[: len(scales)]))

    fig, ax = plt.subplots(figsize=(11, 5))
    x = range(len(scenarios))
    width = 0.25
    for i, scale in enumerate(scales):
        vals = [
            agg[(agg["scale"] == scale) & (agg["scenario"] == s)]["avg_seconds"].values
            for s in scenarios
        ]
        heights = [v[0] if len(v) > 0 else 0 for v in vals]
        offset = (i - len(scales) / 2 + 0.5) * width
        bars = ax.bar([xi + offset for xi in x], heights, width=width,
                      color=colors[scale], label=f"scale={scale:,}", alpha=0.85, edgecolor="white")
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, h * 1.02,
                        f"{h:.4f}s", ha="center", va="bottom", fontsize=7)

    ax.set_xticks(list(x))
    ax.set_xticklabels(scenarios, rotation=20, ha="right", fontsize=9)
    ax.set_ylabel("Średni czas [s]")
    ax.set_title("INSERT – Średni czas wykonania per scenariusz (3 próby)")
    ax.legend(title="Skala danych")
    ax.yaxis.set_minor_locator(mticker.AutoMinorLocator())
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    savefig(fig, out_dir, "insert_avg_time_per_scenario.png", dpi)


def plot_insert_ops_per_sec(df: pd.DataFrame, out_dir: Path, dpi: int) -> None:
    df2 = df[df["ops_per_sec"].notna()].copy()
    agg = df2.groupby(["scale", "scenario"])["ops_per_sec"].mean().reset_index()

    scales = sorted(agg["scale"].unique())
    scenarios = sorted(agg["scenario"].unique())
    colors = dict(zip(scales, SCALE_PALETTE[: len(scales)]))

    fig, ax = plt.subplots(figsize=(11, 5))
    x = range(len(scenarios))
    width = 0.25
    for i, scale in enumerate(scales):
        vals = [
            agg[(agg["scale"] == scale) & (agg["scenario"] == s)]["ops_per_sec"].values
            for s in scenarios
        ]
        heights = [v[0] if len(v) > 0 else 0 for v in vals]
        offset = (i - len(scales) / 2 + 0.5) * width
        ax.bar([xi + offset for xi in x], heights, width=width,
               color=colors[scale], label=f"scale={scale:,}", alpha=0.85, edgecolor="white")

    ax.set_xticks(list(x))
    ax.set_xticklabels(scenarios, rotation=20, ha="right", fontsize=9)
    ax.set_ylabel("Operacje / sekunda")
    ax.set_title("INSERT – Przepustowość (ops/s) per scenariusz")
    ax.legend(title="Skala danych")
    ax.yaxis.set_minor_locator(mticker.AutoMinorLocator())
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    savefig(fig, out_dir, "insert_ops_per_sec.png", dpi)


# ---------------------------------------------------------------------------
# PLOT 3: Before/after index comparison (READ / UPDATE / DELETE)
# ---------------------------------------------------------------------------

def plot_before_after_index(
    df: pd.DataFrame,
    crud_name: str,
    time_col: str,
    out_dir: Path,
    dpi: int,
) -> None:
    """Grouped bar chart: no_indexes vs with_indexes per scenario."""
    agg = df.groupby(["index_mode", "scenario"])[time_col].mean().reset_index()
    agg.rename(columns={time_col: "avg_seconds"}, inplace=True)

    scenarios = sorted(agg["scenario"].unique())
    modes = ["no_indexes", "with_indexes"]
    labels = {"no_indexes": "Bez indeksów", "with_indexes": "Z indeksami"}
    colors = {"no_indexes": COLOR_NO_IDX, "with_indexes": COLOR_WITH_IDX}

    fig, ax = plt.subplots(figsize=(11, 5))
    x = range(len(scenarios))
    width = 0.35

    for i, mode in enumerate(modes):
        vals = [
            agg[(agg["index_mode"] == mode) & (agg["scenario"] == s)]["avg_seconds"].values
            for s in scenarios
        ]
        heights = [v[0] if len(v) > 0 else 0 for v in vals]
        offset = (i - 0.5) * width
        bars = ax.bar(
            [xi + offset for xi in x], heights, width=width,
            color=colors[mode], label=labels[mode], alpha=0.85, edgecolor="white",
        )
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, h * 1.015,
                        f"{h:.4f}", ha="center", va="bottom", fontsize=7)

    ax.set_xticks(list(x))
    ax.set_xticklabels(scenarios, rotation=22, ha="right", fontsize=9)
    ax.set_ylabel("Średni czas [s]")
    ax.set_title(f"{crud_name} – Porównanie przed/po indeksach (avg 3 prób)")
    ax.legend()
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    savefig(fig, out_dir, f"{crud_name.lower()}_before_after_index.png", dpi)


# ---------------------------------------------------------------------------
# PLOT 4: Speedup ratio (with_indexes / no_indexes)
# ---------------------------------------------------------------------------

def plot_speedup(dfs: Dict[str, pd.DataFrame], time_col_map: Dict[str, str], out_dir: Path, dpi: int) -> None:
    """
    Horizontal bar chart showing speedup = time_no_idx / time_with_idx per scenario.
    Values > 1 mean index is faster; values < 1 mean slower (unusual).
    """
    rows = []
    for crud_name, df in dfs.items():
        time_col = time_col_map[crud_name]
        agg = df.groupby(["index_mode", "scenario"])[time_col].mean().unstack("index_mode")
        if "no_indexes" not in agg.columns or "with_indexes" not in agg.columns:
            continue
        agg["speedup"] = agg["no_indexes"] / agg["with_indexes"]
        for scenario, speedup in agg["speedup"].items():
            rows.append({"crud": crud_name, "scenario": scenario, "speedup": speedup})

    if not rows:
        print("  [POMINIĘTO] brak danych do wykresu speedup")
        return

    speedup_df = pd.DataFrame(rows).sort_values("speedup", ascending=True)
    speedup_df["label"] = speedup_df["crud"] + ": " + speedup_df["scenario"]

    colors = [CRUD_COLORS.get(row["crud"], "#888888") for _, row in speedup_df.iterrows()]

    fig, ax = plt.subplots(figsize=(10, max(5, len(speedup_df) * 0.45)))
    bars = ax.barh(speedup_df["label"], speedup_df["speedup"], color=colors, alpha=0.85, edgecolor="white")
    ax.axvline(1.0, color="gray", linestyle="--", linewidth=1, label="brak różnicy (1×)")

    for bar, val in zip(bars, speedup_df["speedup"]):
        ax.text(
            max(val + 0.05, 0.1),
            bar.get_y() + bar.get_height() / 2,
            f"{val:.2f}×",
            va="center", fontsize=8,
        )

    ax.set_xlabel("Przyspieszenie (× razy szybciej z indeksem)")
    ax.set_title("Speedup indeksów – wszystkie scenariusze READ/UPDATE/DELETE")
    ax.legend()
    ax.grid(axis="x", alpha=0.35)
    fig.tight_layout()
    savefig(fig, out_dir, "speedup_all_scenarios.png", dpi)


# ---------------------------------------------------------------------------
# PLOT 5: All 24 scenarios overview (horizontal bar, with_indexes avg time)
# ---------------------------------------------------------------------------

def plot_all_scenarios_overview(
    insert_df: Optional[pd.DataFrame],
    read_df: Optional[pd.DataFrame],
    update_df: Optional[pd.DataFrame],
    delete_df: Optional[pd.DataFrame],
    out_dir: Path,
    dpi: int,
) -> None:
    rows = []

    if insert_df is not None:
        # INSERT – use largest available scale, with no index_mode column
        max_scale = insert_df["scale"].max()
        agg = insert_df[insert_df["scale"] == max_scale].groupby("scenario")["seconds"].mean()
        for scenario, avg_s in agg.items():
            rows.append({"crud": "INSERT", "scenario": scenario, "avg_seconds": avg_s})

    for crud_name, df, time_col in [
        ("READ",   read_df,   "seconds"),
        ("UPDATE", update_df, "seconds"),
        ("DELETE", delete_df, "seconds"),
    ]:
        if df is None:
            continue
        wi = df[df["index_mode"] == "with_indexes"]
        agg = wi.groupby("scenario")[time_col].mean()
        for scenario, avg_s in agg.items():
            rows.append({"crud": crud_name, "scenario": scenario, "avg_seconds": avg_s})

    if not rows:
        print("  [POMINIĘTO] brak danych do overview")
        return

    ov_df = pd.DataFrame(rows).sort_values(["crud", "avg_seconds"])
    ov_df["label"] = ov_df["crud"] + ": " + ov_df["scenario"]
    colors = [CRUD_COLORS.get(row["crud"], "#888888") for _, row in ov_df.iterrows()]

    fig, ax = plt.subplots(figsize=(11, max(6, len(ov_df) * 0.45)))
    bars = ax.barh(ov_df["label"], ov_df["avg_seconds"], color=colors, alpha=0.85, edgecolor="white")

    for bar, val in zip(bars, ov_df["avg_seconds"]):
        ax.text(
            val + val * 0.01 + 1e-6,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.4f}s",
            va="center", fontsize=7.5,
        )

    ax.set_xlabel("Średni czas wykonania [s]")
    ax.set_title("Przegląd wszystkich scenariuszy CRUD – PostgreSQL (z indeksami)")
    ax.grid(axis="x", alpha=0.35)

    # legend for CRUD categories
    from matplotlib.patches import Patch
    legend_patches = [Patch(color=c, label=k) for k, c in CRUD_COLORS.items()]
    ax.legend(handles=legend_patches, loc="lower right")

    fig.tight_layout()
    savefig(fig, out_dir, "all_scenarios_overview.png", dpi)


# ---------------------------------------------------------------------------
# PLOT 6: Heatmap – seconds per scenario x run (quality check)
# ---------------------------------------------------------------------------

def plot_heatmap(df: pd.DataFrame, crud_name: str, time_col: str, out_dir: Path, dpi: int) -> None:
    """Heatmap of raw run times to visualise variance."""
    if "index_mode" in df.columns:
        df = df[df["index_mode"] == "with_indexes"].copy()

    pivot = df.pivot_table(index="scenario", columns="run", values=time_col, aggfunc="mean")

    fig, ax = plt.subplots(figsize=(max(5, len(pivot.columns) + 2), max(4, len(pivot) * 0.6)))
    sns.heatmap(
        pivot, annot=True, fmt=".4f", cmap="YlOrRd",
        linewidths=0.5, ax=ax, cbar_kws={"label": "czas [s]"},
    )
    ax.set_title(f"{crud_name} – Czasy wykonania per próba (heatmap)")
    ax.set_xlabel("Numer próby")
    ax.set_ylabel("Scenariusz")
    fig.tight_layout()
    savefig(fig, out_dir, f"{crud_name.lower()}_heatmap_runs.png", dpi)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Wizualizacja wyników benchmarku PostgreSQL")
    parser.add_argument("--results-dir", default="../postgres/results",
                        help="Katalog z plikami CSV (domyślnie: ../postgres/results)")
    parser.add_argument("--output-dir", default="./charts",
                        help="Katalog wyjściowy dla PNG (domyślnie: ./charts)")
    parser.add_argument("--dpi", type=int, default=150)
    parser.add_argument("--style", default="seaborn-v0_8-whitegrid")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        plt.style.use(args.style)
    except OSError:
        print(f"  [WARN] styl '{args.style}' niedostępny, używam domyślnego.")

    print(f"\n=== Wczytywanie danych z: {results_dir} ===")

    insert_df = load_csv(results_dir / "psql_insert_benchmark_results.csv")
    read_df   = load_csv(results_dir / "psql_read_benchmark_results.csv")
    update_df = load_csv(results_dir / "psql_update_benchmark_results.csv")
    delete_df = load_csv(results_dir / "psql_delete_benchmark_results.csv")

    print(f"\n=== Generowanie wykresów -> {out_dir} ===\n")

    # INSERT charts
    if insert_df is not None:
        print("[1/8] INSERT – czas per scenariusz")
        plot_insert_time_per_scenario(insert_df, out_dir, args.dpi)

        print("[2/8] INSERT – ops/sec per scenariusz")
        plot_insert_ops_per_sec(insert_df, out_dir, args.dpi)

        print("[3/8] INSERT – heatmap prób")
        plot_heatmap(insert_df, "INSERT", "seconds", out_dir, args.dpi)

    # READ charts
    if read_df is not None:
        print("[4/8] READ – przed/po indeksach")
        plot_before_after_index(read_df, "READ", "seconds", out_dir, args.dpi)

        print("[4b]  READ – heatmap prób")
        plot_heatmap(read_df, "READ", "seconds", out_dir, args.dpi)

    # UPDATE charts
    if update_df is not None:
        print("[5/8] UPDATE – przed/po indeksach")
        plot_before_after_index(update_df, "UPDATE", "seconds", out_dir, args.dpi)

        print("[5b]  UPDATE – heatmap prób")
        plot_heatmap(update_df, "UPDATE", "seconds", out_dir, args.dpi)

    # DELETE charts
    if delete_df is not None:
        print("[6/8] DELETE – przed/po indeksach")
        plot_before_after_index(delete_df, "DELETE", "seconds", out_dir, args.dpi)

        print("[6b]  DELETE – heatmap prób")
        plot_heatmap(delete_df, "DELETE", "seconds", out_dir, args.dpi)

    # Speedup chart
    dfs_for_speedup = {}
    time_col_map = {}
    for name, df in [("READ", read_df), ("UPDATE", update_df), ("DELETE", delete_df)]:
        if df is not None:
            dfs_for_speedup[name] = df
            time_col_map[name] = "seconds"

    if dfs_for_speedup:
        print("[7/8] Speedup indeksów – wszystkie scenariusze")
        plot_speedup(dfs_for_speedup, time_col_map, out_dir, args.dpi)

    # All scenarios overview
    print("[8/8] Przegląd wszystkich 24 scenariuszy")
    plot_all_scenarios_overview(insert_df, read_df, update_df, delete_df, out_dir, args.dpi)

    print(f"\nWszystkie wykresy zapisane w: {out_dir.resolve()}")
    print("Gotowe do wklejenia w sprawozdaniu!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
