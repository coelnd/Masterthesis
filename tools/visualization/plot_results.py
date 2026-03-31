"""
Visualize aggregated experiment results from results/summary.json
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ARCH_ORDER = ["baseline", "lambda", "kappa", "datafabric", "datamesh"]
ARCH_LABELS = {
    "baseline": "Baseline",
    "lambda": "Lambda",
    "kappa": "Kappa",
    "datafabric": "Fabric",
    "datamesh": "Mesh",
}
WORKLOAD_ORDER = ["workload_A", "workload_B", "workload_C", "workload_D", "workload_E"]
WORKLOAD_LABELS = {
    "workload_A": "A",
    "workload_B": "B",
    "workload_C": "C",
    "workload_D": "D",
    "workload_E": "E",
}


def _summary_archs(architectures: dict) -> list[str]:
    return [a for a in ARCH_ORDER if a in architectures]


def _summary_workloads(architectures: dict) -> list[str]:
    found: set[str] = set()
    for block in architectures.values():
        if not isinstance(block, dict):
            continue
        for k, v in block.items():
            if k.startswith("workload_") and isinstance(v, dict):
                found.add(k)
    return [w for w in WORKLOAD_ORDER if w in found]


def load_summary(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def arch_workload_grid(
    architectures: dict,
    metric_key: str,
    *,
    subkey: str | None = "p95",
) -> tuple[np.ndarray, list[str], list[str]]:
    archs = _summary_archs(architectures)
    wls = _summary_workloads(architectures)
    values = np.full((len(archs), len(wls)), np.nan)
    for ia, arch in enumerate(archs):
        block = architectures[arch]
        for iw, wl in enumerate(wls):
            if wl not in block:
                continue
            m = block[wl].get(metric_key)
            if m is None:
                continue
            if subkey is None:
                if isinstance(m, (int, float)):
                    values[ia, iw] = float(m)
            elif isinstance(m, dict) and m.get(subkey) is not None:
                values[ia, iw] = float(m[subkey])
    return values, archs, wls


def scalar_per_arch_workload(
    architectures: dict,
    key: str,
    *,
    subkey: str | None = None,
) -> tuple[np.ndarray, list[str], list[str]]:
    archs = _summary_archs(architectures)
    wls = _summary_workloads(architectures)
    out = np.full((len(archs), len(wls)), np.nan)
    for ia, arch in enumerate(archs):
        block = architectures[arch]
        for iw, wl in enumerate(wls):
            if wl not in block:
                continue
            v = block[wl].get(key)
            if v is None:
                continue
            if subkey is None:
                if isinstance(v, (int, float)):
                    out[ia, iw] = float(v)
            elif isinstance(v, dict) and v.get(subkey) is not None:
                out[ia, iw] = float(v[subkey])
    return out, archs, wls


def plot_grouped_bars(
    values: np.ndarray,
    ylabel: str,
    title: str,
    out_path: Path,
    *,
    archs: list[str],
    wls: list[str],
    yscale_linear: bool = True,
) -> None:
    n_arch, n_wl = values.shape
    x = np.arange(n_arch, dtype=float)
    width = min(0.8 / max(n_wl, 1), 0.15)
    fig, ax = plt.subplots(figsize=(10, 5))
    for iw in range(n_wl):
        y = values[:, iw]
        offset = (iw - (n_wl - 1) / 2.0) * width
        ax.bar(
            x + offset,
            np.nan_to_num(y, nan=0.0),
            width,
            label=WORKLOAD_LABELS[wls[iw]],
        )
    ax.set_xticks(x)
    ax.set_xticklabels([ARCH_LABELS[a] for a in archs])
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(title="Workload", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    if not yscale_linear:
        ax.set_yscale("log")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_figure_6_ooo_by_workload(architectures: dict, out_path: Path) -> None:
    """One row of bars per workload: architectures on X."""
    vals, archs, wls = scalar_per_arch_workload(architectures, "out_of_order_rate", subkey=None)
    n_wl = len(wls)
    if n_wl == 0:
        return
    fig, axes = plt.subplots(1, n_wl, figsize=(3 * n_wl, 4), sharey=True)
    if n_wl == 1:
        axes = [axes]
    for iw, wl in enumerate(wls):
        ax = axes[iw]
        y = vals[:, iw]
        x = np.arange(len(archs))
        ax.bar(x, np.nan_to_num(y, nan=0.0))
        ax.set_xticks(x)
        ax.set_xticklabels([ARCH_LABELS[a] for a in archs], rotation=45, ha="right")
        ax.set_title(f"Workload {WORKLOAD_LABELS[wl]}")
        ax.set_ylabel("Out-of-order rate" if iw == 0 else "")
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle("RQ2 / Data quality: Out-of-order rate by workload", fontsize=11)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_opi_oti_bars(architectures: dict, out_path: Path) -> None:
    opi, archs, wls = scalar_per_arch_workload(architectures, "online_performance_index", subkey=None)
    oti, _, _ = scalar_per_arch_workload(architectures, "online_trust_index", subkey=None)
    if not wls:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    for ax, arr, title, ylabel in (
        (ax1, opi, "OPI — online performance index", "OPI"),
        (ax2, oti, "OTI — online trust index", "OTI"),
    ):
        n_arch, n_wl = arr.shape
        x = np.arange(n_arch, dtype=float)
        width = min(0.8 / max(n_wl, 1), 0.15)
        for iw in range(n_wl):
            offset = (iw - (n_wl - 1) / 2.0) * width
            ax.bar(
                x + offset,
                np.nan_to_num(arr[:, iw], nan=0.0),
                width,
                label=WORKLOAD_LABELS[wls[iw]],
            )
        ax.set_xticks(x)
        ax.set_xticklabels([ARCH_LABELS[a] for a in archs], rotation=25, ha="right")
        ax.set_ylim(0.0, 1.05)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", alpha=0.3)
    ax2.legend(title="Workload", fontsize=8, loc="lower right")
    fig.suptitle("Composite indices: performance vs trust (ORS = √(OPI·OTI))", fontsize=11)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_cpu_ram_p95(architectures: dict, out_path: Path) -> None:
    archs = _summary_archs(architectures)
    wls = _summary_workloads(architectures)
    n_a, n_w = len(archs), len(wls)
    if n_w == 0:
        return
    cpu_p95 = np.full((n_a, n_w), np.nan)
    ram_p95 = np.full((n_a, n_w), np.nan)
    for ia, arch in enumerate(archs):
        block = architectures[arch]
        for iw, wl in enumerate(wls):
            if wl not in block:
                continue
            hm = block[wl].get("hardware_metrics") or {}
            inner = hm.get("hardware_metrics") if isinstance(hm, dict) else None
            if not isinstance(inner, dict):
                continue
            cpu = inner.get("cpu_usage_percent") or {}
            ram = inner.get("memory_usage_gb") or {}
            if isinstance(cpu, dict) and cpu.get("p95") is not None:
                cpu_p95[ia, iw] = float(cpu["p95"])
            if isinstance(ram, dict) and ram.get("p95") is not None:
                ram_p95[ia, iw] = float(ram["p95"])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5), sharey=False)
    for ax, arr, title, ylabel in (
        (ax1, cpu_p95, "CPU usage (p95)", "CPU usage p95 (%)"),
        (ax2, ram_p95, "RAM usage (p95)", "RAM usage p95 (GB)"),
    ):
        n_arch, n_wl = arr.shape
        x = np.arange(n_arch, dtype=float)
        width = min(0.8 / max(n_wl, 1), 0.15)
        for iw in range(n_wl):
            offset = (iw - (n_wl - 1) / 2.0) * width
            ax.bar(
                x + offset,
                np.nan_to_num(arr[:, iw], nan=0.0),
                width,
                label=WORKLOAD_LABELS[wls[iw]],
            )
        ax.set_xticks(x)
        ax.set_xticklabels([ARCH_LABELS[a] for a in archs], rotation=25, ha="right")
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", alpha=0.3)
    ax2.legend(title="Workload", fontsize=8, loc="upper right")
    fig.suptitle("Hardware metrics (p95) from Prometheus", fontsize=11)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def plot_ml_f1_optional_precision_recall(architectures: dict, out_path: Path) -> None:
    f1, archs, wls = scalar_per_arch_workload(architectures, "ml_f1", subkey=None)
    prec, _, _ = scalar_per_arch_workload(architectures, "ml_precision", subkey=None)
    rec, _, _ = scalar_per_arch_workload(architectures, "ml_recall", subkey=None)
    if not wls:
        return
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    titles = ["ML F1", "Precision", "Recall"]
    arrs = [f1, prec, rec]
    for ax, arr, title in zip(axes, arrs, titles):
        n_arch, n_wl = arr.shape
        x = np.arange(n_arch, dtype=float)
        width = min(0.8 / max(n_wl, 1), 0.15)
        for iw in range(n_wl):
            offset = (iw - (n_wl - 1) / 2.0) * width
            ax.bar(
                x + offset,
                np.nan_to_num(arr[:, iw], nan=0.0),
                width,
                label=WORKLOAD_LABELS[wls[iw]],
            )
        ax.set_xticks(x)
        ax.set_xticklabels([ARCH_LABELS[a] for a in archs])
        ax.set_ylim(0, 1.05)
        ax.set_title(title)
        ax.grid(axis="y", alpha=0.3)
    axes[0].set_ylabel("Score")
    axes[1].legend(title="Workload", fontsize=7, loc="lower right")
    fig.suptitle("RQ3: ML suitability (rule-based proxy metrics)", fontsize=11)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def main() -> None:
    ap = argparse.ArgumentParser(description="Plot thesis figures from summary.json")
    ap.add_argument(
        "--summary",
        type=Path,
        default=Path("results/summary.json"),
        help="Path to summary.json",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=Path("results/figures"),
        help="Output directory for PNG files",
    )
    args = ap.parse_args()
    data = load_summary(args.summary)
    architectures = data["architectures"]

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if not _summary_archs(architectures) or not _summary_workloads(architectures):
        print("No architecture/workload data in summary; skipping plots.")
        return

    v, ga, gw = arch_workload_grid(architectures, "fal_total_window_s", subkey="p95")
    plot_grouped_bars(
        v,
        "FAL total window p95 (s)",
        "RQ1: Timeliness — FAL total window (p95)",
        out_dir / "01_fal_total_window_p95.png",
        archs=ga,
        wls=gw,
    )

    v, ga, gw = arch_workload_grid(architectures, "availability_latency_s", subkey="p95")
    plot_grouped_bars(
        v,
        "Availability latency p95 (s)",
        "RQ1: Timeliness — availability latency (p95)",
        out_dir / "02_availability_latency_p95.png",
        archs=ga,
        wls=gw,
    )

    v, ga, gw = arch_workload_grid(architectures, "flag_flip_rate_per_hour", subkey="p95")
    plot_grouped_bars(
        v,
        "Flag flip rate p95 (1/h)",
        "RQ2: Stability — flag flip rate (p95)",
        out_dir / "03_flag_flip_rate_p95.png",
        archs=ga,
        wls=gw,
    )

    v, ga, gw = arch_workload_grid(architectures, "mtbf_minutes", subkey="p95")
    plot_grouped_bars(
        v,
        "MTBF p95 (minutes)",
        "RQ2: Stability — mean time between flips (p95)",
        out_dir / "04_mtbf_minutes_p95.png",
        archs=ga,
        wls=gw,
    )

    plot_ml_f1_optional_precision_recall(architectures, out_dir / "05_ml_f1_precision_recall.png")

    plot_figure_6_ooo_by_workload(architectures, out_dir / "06_out_of_order_rate_by_workload.png")

    v, ga, gw = arch_workload_grid(architectures, "window_integrity_score_5min", subkey="p95")
    plot_grouped_bars(
        v,
        "Window integrity p95 (5 min)",
        "Data quality: window integrity score (p95)",
        out_dir / "07_window_integrity_p95.png",
        archs=ga,
        wls=gw,
    )

    v_ors, ga, gw = scalar_per_arch_workload(architectures, "online_readiness_score", subkey=None)
    plot_grouped_bars(
        v_ors,
        "ORS",
        "Overall: online readiness score (ORS)",
        out_dir / "08_online_readiness_score.png",
        archs=ga,
        wls=gw,
    )
    plot_opi_oti_bars(architectures, out_dir / "09_opi_oti_bars.png")
    plot_cpu_ram_p95(architectures, out_dir / "10_cpu_ram_p95.png")

    print(f"Wrote figures to {out_dir.resolve()}")

if __name__ == "__main__":
    main()