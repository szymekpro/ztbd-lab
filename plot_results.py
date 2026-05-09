"""plot_results.py

Skrypt wizualizacji wyników benchmarków (Cassandra / MariaDB / MongoDB / PostgreSQL).

Wymagania:
  - uruchamiany z katalogu głównego repozytorium
  - generuje wykresy dla wszystkich baz danych „od razu”
  - ignoruje brakujące pliki wyników / brakujące kolumny (nie przerywa pracy)
  - zapisuje wykresy do: visualization/[baza_danych]/

Użycie:
  py plot_results.py

Opcjonalnie:
  py plot_results.py --dpi 200 --style seaborn-v0_8-whitegrid --scale 1000000

Zależności (w środowisku Pythona):
  pip install pandas matplotlib seaborn

CSV expected schema (as produced by benchmarks):
  scale,index_mode,scenario,run,seconds,operations,ops_per_sec

"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _require_plot_deps() -> None:
    global plt, mticker, pd, sns
    try:
        import matplotlib.pyplot as _plt  # type: ignore
        import matplotlib.ticker as _mticker  # type: ignore
        import pandas as _pd  # type: ignore
        import seaborn as _sns  # type: ignore
    except ModuleNotFoundError as e:
        missing = getattr(e, "name", "<unknown>")
        raise SystemExit(
            "Brak zależności do wykresów (pandas/matplotlib/seaborn).\n"
            "Zainstaluj je np.:\n"
            "  pip install pandas matplotlib seaborn\n"
            f"Brakujący moduł: {missing}"
        ) from e

    plt = _plt
    mticker = _mticker
    pd = _pd
    sns = _sns


# Lazy-loaded plotting deps (set by _require_plot_deps)
plt = None  # type: ignore
mticker = None  # type: ignore
pd = None  # type: ignore
sns = None  # type: ignore

# Type aliases (avoid referencing lazy globals in annotations)
DataFrame = Any
Figure = Any
Axes = Any


# ---------------------------------------------------------------------------
# PALETTE
# ---------------------------------------------------------------------------

COLOR_NO_IDX = "#e07070"  # czerwony – bez indeksów
COLOR_WITH_IDX = "#5ba85b"  # zielony  – z indeksami

CRUD_COLORS = {
    "INSERT": "#4c78a8",
    "READ": "#54a24b",
    "UPDATE": "#f58518",
    "DELETE": "#e45756",
}

SCALE_PALETTE = ["#4c78a8", "#72b7b2", "#ff9da6"]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


def load_csv(path: Path) -> Optional[DataFrame]:
    if not path.exists():
        print(f"  [POMINIĘTO] brak pliku: {path}")
        return None
    df = pd.read_csv(path)
    print(f"  [OK] wczytano {len(df)} wierszy z {path.name}")
    return df


def _has_columns(df: DataFrame, required: Iterable[str], label: str) -> bool:
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"  [POMINIĘTO] {label}: brak kolumn: {missing}")
        return False
    return True


def _ensure_non_empty(df: Optional[DataFrame], label: str) -> bool:
    if df is None:
        print(f"  [POMINIĘTO] {label}: brak danych (None)")
        return False
    if getattr(df, "empty", False):
        print(f"  [POMINIĘTO] {label}: pusty DataFrame")
        return False
    return True


def _filter_scale(df: DataFrame, scale: Optional[int], label: str) -> DataFrame:
    if "scale" not in df.columns:
        return df
    if df.empty:
        return df

    unique_scales = sorted(df["scale"].dropna().unique())
    if not unique_scales:
        return df

    chosen = scale
    if chosen is None:
        if len(unique_scales) > 1:
            chosen = int(max(unique_scales))
            print(f"  [INFO] {label}: wykryto wiele skal {unique_scales}; używam domyślnie największej: {chosen}")
        else:
            chosen = int(unique_scales[0])

    filtered = df[df["scale"] == chosen].copy()
    if filtered.empty:
        print(
            f"  [WARN] {label}: brak danych dla scale={chosen}; dostępne: {unique_scales}. "
            "Używam danych bez filtrowania."
        )
        return df
    return filtered


def savefig(fig: Figure, out_dir: Path, name: str, dpi: int) -> None:
    out_path = out_dir / name
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    print(f"  -> zapisano: {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# PLOT 1 & 2: INSERT – avg time and ops/sec per scenario (3 runs averaged)
# ---------------------------------------------------------------------------


def plot_insert_time_per_scenario(df: DataFrame, out_dir: Path, dpi: int, db_label: str) -> None:
    label = f"{db_label} / INSERT avg time"
    if not _ensure_non_empty(df, label):
        return
    if not _has_columns(df, ["scenario", "seconds", "scale"], label):
        return

    df2 = df.copy()
    if "index_mode" in df2.columns and "with_indexes" in set(df2["index_mode"].dropna().unique()):
        df2 = df2[df2["index_mode"] == "with_indexes"].copy()

    agg = df2.groupby(["scale", "scenario"])["seconds"].mean().reset_index()
    if agg.empty:
        print(f"  [POMINIĘTO] {label}: brak danych po agregacji")
        return
    agg.rename(columns={"seconds": "avg_seconds"}, inplace=True)

    scales = sorted(agg["scale"].unique())
    scenarios = sorted(agg["scenario"].unique())
    colors = dict(zip(scales, SCALE_PALETTE[: len(scales)]))

    fig, ax = plt.subplots(figsize=(11, 5))
    x = range(len(scenarios))
    width = 0.25 if len(scales) <= 3 else max(0.12, 0.8 / len(scales))

    for i, scale in enumerate(scales):
        vals = [
            agg[(agg["scale"] == scale) & (agg["scenario"] == s)]["avg_seconds"].values
            for s in scenarios
        ]
        heights = [v[0] if len(v) > 0 else 0 for v in vals]
        offset = (i - len(scales) / 2 + 0.5) * width
        bars = ax.bar(
            [xi + offset for xi in x],
            heights,
            width=width,
            color=colors.get(scale, "#888888"),
            label=f"scale={int(scale):,}",
            alpha=0.85,
            edgecolor="white",
        )
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    h * 1.02,
                    f"{h:.4f}s",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                )

    ax.set_xticks(list(x))
    ax.set_xticklabels(scenarios, rotation=20, ha="right", fontsize=9)
    ax.set_ylabel("Średni czas [s]")
    ax.set_title(f"{db_label} – INSERT: Średni czas wykonania per scenariusz (3 próby)")
    ax.legend(title="Skala danych")
    ax.yaxis.set_minor_locator(mticker.AutoMinorLocator())
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    savefig(fig, out_dir, "insert_avg_time_per_scenario.png", dpi)


def plot_insert_ops_per_sec(df: DataFrame, out_dir: Path, dpi: int, db_label: str) -> None:
    label = f"{db_label} / INSERT ops/s"
    if not _ensure_non_empty(df, label):
        return
    if not _has_columns(df, ["scenario", "scale", "ops_per_sec"], label):
        return

    df2 = df.copy()
    if "index_mode" in df2.columns and "with_indexes" in set(df2["index_mode"].dropna().unique()):
        df2 = df2[df2["index_mode"] == "with_indexes"].copy()

    df2 = df2[df2["ops_per_sec"].notna()].copy()
    if df2.empty:
        print(f"  [POMINIĘTO] {label}: brak wartości ops_per_sec")
        return

    agg = df2.groupby(["scale", "scenario"])["ops_per_sec"].mean().reset_index()
    if agg.empty:
        print(f"  [POMINIĘTO] {label}: brak danych po agregacji")
        return

    scales = sorted(agg["scale"].unique())
    scenarios = sorted(agg["scenario"].unique())
    colors = dict(zip(scales, SCALE_PALETTE[: len(scales)]))

    fig, ax = plt.subplots(figsize=(11, 5))
    x = range(len(scenarios))
    width = 0.25 if len(scales) <= 3 else max(0.12, 0.8 / len(scales))

    for i, scale in enumerate(scales):
        vals = [
            agg[(agg["scale"] == scale) & (agg["scenario"] == s)]["ops_per_sec"].values
            for s in scenarios
        ]
        heights = [v[0] if len(v) > 0 else 0 for v in vals]
        offset = (i - len(scales) / 2 + 0.5) * width
        ax.bar(
            [xi + offset for xi in x],
            heights,
            width=width,
            color=colors.get(scale, "#888888"),
            label=f"scale={int(scale):,}",
            alpha=0.85,
            edgecolor="white",
        )

    ax.set_xticks(list(x))
    ax.set_xticklabels(scenarios, rotation=20, ha="right", fontsize=9)
    ax.set_ylabel("Operacje / sekunda")
    ax.set_title(f"{db_label} – INSERT: Przepustowość (ops/s) per scenariusz")
    ax.legend(title="Skala danych")
    ax.yaxis.set_minor_locator(mticker.AutoMinorLocator())
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    savefig(fig, out_dir, "insert_ops_per_sec.png", dpi)


# ---------------------------------------------------------------------------
# PLOT X: CRUD avg time per scenario grouped by scale (READ/UPDATE/DELETE)
# ---------------------------------------------------------------------------


def _plot_time_per_scenario_grouped_by_scale(
    df: DataFrame,
    crud_name: str,
    out_dir: Path,
    dpi: int,
    db_label: str,
) -> None:
    label = f"{db_label} / {crud_name} avg time per scenario (scales)"
    if not _ensure_non_empty(df, label):
        return
    if not _has_columns(df, ["scale", "scenario", "seconds"], label):
        return

    df2 = df.copy()
    if "index_mode" in df2.columns and "with_indexes" in set(df2["index_mode"].dropna().unique()):
        df2 = df2[df2["index_mode"] == "with_indexes"].copy()

    agg = df2.groupby(["scale", "scenario"])["seconds"].mean().reset_index()
    if agg.empty:
        print(f"  [POMINIĘTO] {label}: brak danych po agregacji")
        return

    agg.rename(columns={"seconds": "avg_seconds"}, inplace=True)

    scales = sorted(agg["scale"].unique())
    scenarios = sorted(agg["scenario"].unique())
    colors = dict(zip(scales, SCALE_PALETTE[: len(scales)]))

    fig, ax = plt.subplots(figsize=(11, 5))
    x = range(len(scenarios))
    width = 0.25 if len(scales) <= 3 else max(0.12, 0.8 / len(scales))

    for i, scale in enumerate(scales):
        vals = [
            agg[(agg["scale"] == scale) & (agg["scenario"] == s)]["avg_seconds"].values
            for s in scenarios
        ]
        heights = [v[0] if len(v) > 0 else 0 for v in vals]
        offset = (i - len(scales) / 2 + 0.5) * width
        bars = ax.bar(
            [xi + offset for xi in x],
            heights,
            width=width,
            color=colors.get(scale, "#888888"),
            label=f"scale={int(scale):,}",
            alpha=0.85,
            edgecolor="white",
        )
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    h * 1.02,
                    f"{h:.4f}s",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                )

    ax.set_xticks(list(x))
    ax.set_xticklabels(scenarios, rotation=20, ha="right", fontsize=9)
    ax.set_ylabel("Średni czas [s]")
    ax.set_title(f"{db_label} – {crud_name}: Średni czas wykonania per scenariusz (z indeksami)")
    ax.legend(title="Skala danych")
    ax.yaxis.set_minor_locator(mticker.AutoMinorLocator())
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()

    name_map = {
        "READ": "read_avg_time_per_scenario.png",
        "UPDATE": "update_avg_time_per_scenario.png",
        "DELETE": "delete_avg_time_per_scenario.png",
    }
    savefig(fig, out_dir, name_map[crud_name], dpi)


def plot_read_time_per_scenario(df: DataFrame, out_dir: Path, dpi: int, db_label: str) -> None:
    _plot_time_per_scenario_grouped_by_scale(df, "READ", out_dir, dpi, db_label)


def plot_update_time_per_scenario(df: DataFrame, out_dir: Path, dpi: int, db_label: str) -> None:
    _plot_time_per_scenario_grouped_by_scale(df, "UPDATE", out_dir, dpi, db_label)


def plot_delete_time_per_scenario(df: DataFrame, out_dir: Path, dpi: int, db_label: str) -> None:
    _plot_time_per_scenario_grouped_by_scale(df, "DELETE", out_dir, dpi, db_label)


# ---------------------------------------------------------------------------
# PLOT 3: Before/after index comparison (READ / UPDATE / DELETE)
# ---------------------------------------------------------------------------


def plot_before_after_index(
    df: DataFrame,
    crud_name: str,
    time_col: str,
    out_dir: Path,
    dpi: int,
    db_label: str,
) -> None:
    """Grouped bar chart: no_indexes vs with_indexes per scenario."""
    label = f"{db_label} / {crud_name} before-after indexes"
    if not _ensure_non_empty(df, label):
        return
    if not _has_columns(df, ["index_mode", "scenario", time_col], label):
        return

    modes_present = set(df["index_mode"].dropna().unique())
    required_modes = {"no_indexes", "with_indexes"}
    if not required_modes.issubset(modes_present):
        print(f"  [POMINIĘTO] {label}: brak trybów index_mode {sorted(required_modes)} (jest: {sorted(modes_present)})")
        return

    agg = df.groupby(["index_mode", "scenario"])[time_col].mean().reset_index()
    if agg.empty:
        print(f"  [POMINIĘTO] {label}: brak danych po agregacji")
        return

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
            [xi + offset for xi in x],
            heights,
            width=width,
            color=colors[mode],
            label=labels[mode],
            alpha=0.85,
            edgecolor="white",
        )
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    h * 1.015,
                    f"{h:.4f}",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                )

    ax.set_xticks(list(x))
    ax.set_xticklabels(scenarios, rotation=22, ha="right", fontsize=9)
    ax.set_ylabel("Średni czas [s]")
    ax.set_title(f"{db_label} – {crud_name}: Porównanie przed/po indeksach (avg 3 prób)")
    ax.legend()
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    savefig(fig, out_dir, f"{crud_name.lower()}_before_after_index.png", dpi)


# ---------------------------------------------------------------------------
# PLOT 4: Speedup ratio (with_indexes / no_indexes)
# ---------------------------------------------------------------------------


def plot_speedup(
    dfs: Dict[str, DataFrame],
    time_col_map: Dict[str, str],
    out_dir: Path,
    dpi: int,
    db_label: str,
) -> None:
    """Horizontal bar chart: speedup = time_no_idx / time_with_idx per scenario."""
    rows: List[Dict[str, Any]] = []

    for crud_name, df in dfs.items():
        label = f"{db_label} / speedup {crud_name}"
        if not _ensure_non_empty(df, label):
            continue
        time_col = time_col_map[crud_name]
        if not _has_columns(df, ["index_mode", "scenario", time_col], label):
            continue

        agg = df.groupby(["index_mode", "scenario"])[time_col].mean().unstack("index_mode")
        if agg.empty:
            continue
        if "no_indexes" not in agg.columns or "with_indexes" not in agg.columns:
            continue

        agg["speedup"] = agg["no_indexes"] / agg["with_indexes"]
        for scenario, speedup in agg["speedup"].items():
            rows.append({"crud": crud_name, "scenario": scenario, "speedup": speedup})

    if not rows:
        print(f"  [POMINIĘTO] {db_label} / speedup: brak danych")
        return

    speedup_df = pd.DataFrame(rows).sort_values("speedup", ascending=True)
    speedup_df["label"] = speedup_df["crud"] + ": " + speedup_df["scenario"]

    colors = [CRUD_COLORS.get(row["crud"], "#888888") for _, row in speedup_df.iterrows()]

    fig, ax = plt.subplots(figsize=(10, max(5, len(speedup_df) * 0.45)))
    bars = ax.barh(speedup_df["label"], speedup_df["speedup"], color=colors, alpha=0.85, edgecolor="white")
    ax.axvline(1.0, color="gray", linestyle="--", linewidth=1, label="brak różnicy (1×)")

    for bar, val in zip(bars, speedup_df["speedup"]):
        ax.text(max(val + 0.05, 0.1), bar.get_y() + bar.get_height() / 2, f"{val:.2f}×", va="center", fontsize=8)

    ax.set_xlabel("Przyspieszenie (× razy szybciej z indeksem)")
    ax.set_title(f"{db_label} – Speedup indeksów: READ/UPDATE/DELETE")
    ax.legend()
    ax.grid(axis="x", alpha=0.35)
    fig.tight_layout()
    savefig(fig, out_dir, "speedup_all_scenarios.png", dpi)


# ---------------------------------------------------------------------------
# PLOT 5: All scenarios overview (horizontal bar)
# ---------------------------------------------------------------------------


def plot_all_scenarios_overview(
    insert_df: Optional[DataFrame],
    read_df: Optional[DataFrame],
    update_df: Optional[DataFrame],
    delete_df: Optional[DataFrame],
    out_dir: Path,
    dpi: int,
    db_label: str,
) -> None:
    rows: List[Dict[str, Any]] = []

    if insert_df is not None and not getattr(insert_df, "empty", False):
        df2 = insert_df
        if "index_mode" in df2.columns and "with_indexes" in set(df2["index_mode"].dropna().unique()):
            df2 = df2[df2["index_mode"] == "with_indexes"].copy()

        if "scale" in df2.columns and "scenario" in df2.columns and "seconds" in df2.columns:
            max_scale = df2["scale"].max()
            agg = df2[df2["scale"] == max_scale].groupby("scenario")["seconds"].mean()
            for scenario, avg_s in agg.items():
                rows.append({"crud": "INSERT", "scenario": scenario, "avg_seconds": avg_s})

    for crud_name, df, time_col in [
        ("READ", read_df, "seconds"),
        ("UPDATE", update_df, "seconds"),
        ("DELETE", delete_df, "seconds"),
    ]:
        if df is None or getattr(df, "empty", False):
            continue
        if "scenario" not in df.columns or time_col not in df.columns:
            continue

        df2 = df
        if "index_mode" in df2.columns and "with_indexes" in set(df2["index_mode"].dropna().unique()):
            df2 = df2[df2["index_mode"] == "with_indexes"].copy()

        agg = df2.groupby("scenario")[time_col].mean()
        for scenario, avg_s in agg.items():
            rows.append({"crud": crud_name, "scenario": scenario, "avg_seconds": avg_s})

    if not rows:
        print(f"  [POMINIĘTO] {db_label} / overview: brak danych")
        return

    ov_df = pd.DataFrame(rows).sort_values(["crud", "avg_seconds"])
    ov_df["label"] = ov_df["crud"] + ": " + ov_df["scenario"]
    colors = [CRUD_COLORS.get(row["crud"], "#888888") for _, row in ov_df.iterrows()]

    fig, ax = plt.subplots(figsize=(11, max(6, len(ov_df) * 0.45)))
    bars = ax.barh(ov_df["label"], ov_df["avg_seconds"], color=colors, alpha=0.85, edgecolor="white")

    for bar, val in zip(bars, ov_df["avg_seconds"]):
        ax.text(val + val * 0.01 + 1e-6, bar.get_y() + bar.get_height() / 2, f"{val:.4f}s", va="center", fontsize=7.5)

    ax.set_xlabel("Średni czas wykonania [s]")
    ax.set_title(f"{db_label} – Przegląd scenariuszy CRUD (preferuj z indeksami jeśli dostępne)")
    ax.grid(axis="x", alpha=0.35)

    from matplotlib.patches import Patch  # type: ignore

    legend_patches = [Patch(color=c, label=k) for k, c in CRUD_COLORS.items()]
    ax.legend(handles=legend_patches, loc="lower right")

    fig.tight_layout()
    savefig(fig, out_dir, "all_scenarios_overview.png", dpi)


# ---------------------------------------------------------------------------
# PLOT 6: Heatmap – seconds per scenario x run
# ---------------------------------------------------------------------------


def plot_heatmap(df: DataFrame, crud_name: str, time_col: str, out_dir: Path, dpi: int, db_label: str) -> None:
    label = f"{db_label} / {crud_name} heatmap"
    if not _ensure_non_empty(df, label):
        return
    if not _has_columns(df, ["scenario", "run", time_col], label):
        return

    df2 = df
    if "index_mode" in df2.columns and "with_indexes" in set(df2["index_mode"].dropna().unique()):
        df2 = df2[df2["index_mode"] == "with_indexes"].copy()

    pivot = df2.pivot_table(index="scenario", columns="run", values=time_col, aggfunc="mean")
    if pivot.empty:
        print(f"  [POMINIĘTO] {label}: brak danych po pivot")
        return
    if pivot.isna().all().all():
        print(f"  [POMINIĘTO] {label}: same NaN")
        return

    fig, ax = plt.subplots(figsize=(max(5, len(pivot.columns) + 2), max(4, len(pivot) * 0.6)))
    sns.heatmap(
        pivot,
        annot=True,
        fmt=".4f",
        cmap="YlOrRd",
        linewidths=0.5,
        ax=ax,
        cbar_kws={"label": "czas [s]"},
    )
    ax.set_title(f"{db_label} – {crud_name}: Czasy wykonania per próba (heatmap)")
    ax.set_xlabel("Numer próby")
    ax.set_ylabel("Scenariusz")
    fig.tight_layout()
    savefig(fig, out_dir, f"{crud_name.lower()}_heatmap_runs.png", dpi)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _db_configs() -> List[Tuple[str, str, str]]:
    return [
        ("postgres", "PostgreSQL", "psql"),
        ("mariadb", "MariaDB", "mariadb"),
        ("mongodb", "MongoDB", "mongodb"),
        ("cassandra", "Cassandra", "cassandra"),
    ]


def process_database(repo_root: Path, db_key: str, db_label: str, csv_prefix: str, args: argparse.Namespace) -> None:
    results_dir = repo_root / db_key / "results"
    out_dir = repo_root / "visualization" / db_key

    print(f"\n=== {db_label} ({db_key}) ===")
    print(f"Results: {results_dir}")
    print(f"Output:  {out_dir}")

    insert_df = load_csv(results_dir / f"{csv_prefix}_insert_benchmark_results.csv")
    read_df = load_csv(results_dir / f"{csv_prefix}_read_benchmark_results.csv")
    update_df = load_csv(results_dir / f"{csv_prefix}_update_benchmark_results.csv")
    delete_df = load_csv(results_dir / f"{csv_prefix}_delete_benchmark_results.csv")

    if insert_df is None and read_df is None and update_df is None and delete_df is None:
        print(f"  [POMINIĘTO] {db_label}: brak plików wyników")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    # Keep raw frames for multi-scale plots; create single-scale views for charts that
    # assume one scale (before-after, speedup, overview).
    insert_df_multi = insert_df
    read_df_multi = read_df
    update_df_multi = update_df
    delete_df_multi = delete_df

    insert_df_single = None
    if insert_df is not None:
        insert_df_single = _filter_scale(insert_df, args.scale, f"{db_label} / INSERT")

    read_df_single = _filter_scale(read_df, args.scale, f"{db_label} / READ") if read_df is not None else None
    update_df_single = _filter_scale(update_df, args.scale, f"{db_label} / UPDATE") if update_df is not None else None
    delete_df_single = _filter_scale(delete_df, args.scale, f"{db_label} / DELETE") if delete_df is not None else None

    print(f"\n--- Generowanie wykresów ({db_label}) ---")

    # INSERT
    if insert_df_multi is not None:
        plot_insert_time_per_scenario(insert_df_multi, out_dir, args.dpi, db_label)
        plot_insert_ops_per_sec(insert_df_multi, out_dir, args.dpi, db_label)

        if insert_df_single is not None:
            plot_heatmap(insert_df_single, "INSERT", "seconds", out_dir, args.dpi, db_label)

    # READ
    if read_df_multi is not None:
        plot_read_time_per_scenario(read_df_multi, out_dir, args.dpi, db_label)

    if read_df_single is not None:
        plot_before_after_index(read_df_single, "READ", "seconds", out_dir, args.dpi, db_label)
        plot_heatmap(read_df_single, "READ", "seconds", out_dir, args.dpi, db_label)

    # UPDATE
    if update_df_multi is not None:
        plot_update_time_per_scenario(update_df_multi, out_dir, args.dpi, db_label)

    if update_df_single is not None:
        plot_before_after_index(update_df_single, "UPDATE", "seconds", out_dir, args.dpi, db_label)
        plot_heatmap(update_df_single, "UPDATE", "seconds", out_dir, args.dpi, db_label)

    # DELETE
    if delete_df_multi is not None:
        plot_delete_time_per_scenario(delete_df_multi, out_dir, args.dpi, db_label)

    if delete_df_single is not None:
        plot_before_after_index(delete_df_single, "DELETE", "seconds", out_dir, args.dpi, db_label)
        plot_heatmap(delete_df_single, "DELETE", "seconds", out_dir, args.dpi, db_label)

    # Speedup
    dfs_for_speedup: Dict[str, DataFrame] = {}
    time_col_map: Dict[str, str] = {}

    for name, df in [("READ", read_df_single), ("UPDATE", update_df_single), ("DELETE", delete_df_single)]:
        if df is not None:
            dfs_for_speedup[name] = df
            time_col_map[name] = "seconds"

    if dfs_for_speedup:
        plot_speedup(dfs_for_speedup, time_col_map, out_dir, args.dpi, db_label)

    # Overview
    plot_all_scenarios_overview(insert_df_single, read_df_single, update_df_single, delete_df_single, out_dir, args.dpi, db_label)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description="Wizualizacja wyników benchmarków (wszystkie bazy)")
    parser.add_argument("--dpi", type=int, default=150)
    parser.add_argument("--style", default="seaborn-v0_8-whitegrid")
    parser.add_argument(
        "--scale",
        type=int,
        default=None,
        help=(
            "Jeśli plik CSV zawiera kolumnę 'scale', filtruje dane do podanej skali. "
            "Bez tej flagi, dla wykresów single-scale używana jest największa dostępna skala."
        ),
    )
    args = parser.parse_args()

    _require_plot_deps()

    try:
        plt.style.use(args.style)
    except OSError:
        print(f"  [WARN] styl '{args.style}' niedostępny, używam domyślnego.")

    repo_root = Path(__file__).resolve().parent
    print(f"Repo root: {repo_root}")

    for db_key, db_label, prefix in _db_configs():
        process_database(repo_root, db_key, db_label, prefix, args)

    print("\nGotowe.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
