#!/usr/bin/env python3
"""Build production-ready flight delay aggregates and Weibull fits.

This script ingests BTS on-time performance CSV extracts, normalises the schema,
materialises a consolidated parquet (`data_big.parquet`), and prepares the
`chrome*.parquet` files used downstream. A pickled dictionary containing all
three chrome datasets is also emitted as `chrome.pkl`.

Example:
    python concat_data.py --data-dir data --output-dir . --n-cores 12
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import pickle
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import numpy as np
import polars as pl
from joblib import Parallel, delayed
from polars import col
from scipy.optimize import minimize
from scipy.stats import weibull_min
from tqdm.auto import tqdm


FINAL_COLUMNS: List[str] = [
    "Flying_Airline",
    "Airline",
    "FlightDate",
    "DayOfWeek",
    "Origin",
    "CRSDepTime",
    "DepDelay",
    "Dest",
    "ArrDelay",
    "Cancelled",
    "ActualElapsedTime",
    "TaxiIn",
    "TaxiOut",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
    "CancellationCode",
    "Div1Airport",
]

TYPE_MAP = {
    "Flying_Airline": pl.String,
    "Airline": pl.String,
    "FlightDate": pl.String,
    "DayOfWeek": pl.Int64,
    "Origin": pl.String,
    "CRSDepTime": pl.Int64,
    "DepDelay": pl.Float64,
    "Dest": pl.String,
    "ArrDelay": pl.Float64,
    "Cancelled": pl.Float64,
    "ActualElapsedTime": pl.Float64,
    "TaxiIn": pl.Float64,
    "TaxiOut": pl.Float64,
    "CarrierDelay": pl.Float64,
    "WeatherDelay": pl.Float64,
    "NASDelay": pl.Float64,
    "SecurityDelay": pl.Float64,
    "LateAircraftDelay": pl.Float64,
    "CancellationCode": pl.String,
    "Div1Airport": pl.String,
}

STRING_NULL_COLUMNS = ["CancellationCode", "Div1Airport"]
GROUP_COLUMNS = ["Airline", "Hour", "Month", "Origin", "DayOfWeek"]
DEFAULT_PARQUET = "data_big.parquet"
CHROME_LEVELS = (5, 4, 3)
CHROME_PICKLE = "chrome.pkl"
WEIBULL_BOUNDS = ((0.5, 8.0), (1.0, 100.0))
DEFAULT_WEIBULL_RESULT = (2.0, 10.0)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Aggregate flight delay data and fit Weibull parameters."
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        type=Path,
        help="Directory containing input BTS CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default=Path("."),
        type=Path,
        help="Directory where parquet and pickle outputs will be written.",
    )
    parser.add_argument(
        "--min-records",
        default=30,
        type=int,
        help="Minimum number of flights required for a group to be kept.",
    )
    parser.add_argument(
        "--n-cores",
        default=None,
        type=int,
        help="Number of parallel workers to use for Weibull fitting (defaults to CPU count - 1).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing parquet outputs instead of reusing them if present.",
    )
    return parser.parse_args()


def discover_csv_files(data_dir: Path) -> List[Path]:
    """Return all CSV paths in the provided directory sorted alphabetically."""

    if not data_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {data_dir}")

    csv_files = sorted(p for p in data_dir.iterdir() if p.suffix.lower() == ".csv")
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    return csv_files


def read_and_normalize_csv(csv_path: Path) -> pl.DataFrame:
    """Load a single CSV and coerce it onto the canonical schema."""

    raw = pl.read_csv(
        csv_path,
        truncate_ragged_lines=True,
        ignore_errors=True,
        use_pyarrow=False,
    )

    rename_map = {}
    if "IATA_CODE_Reporting_Airline" in raw.columns:
        rename_map["IATA_CODE_Reporting_Airline"] = "Flying_Airline"
    if "Operating_Airline " in raw.columns:
        rename_map["Operating_Airline "] = "Flying_Airline"
    if rename_map:
        raw = raw.rename(rename_map)

    if "Marketing_Airline_Network" in raw.columns:
        raw = raw.with_columns(col("Marketing_Airline_Network").alias("Airline"))
    elif "Reporting_Airline" in raw.columns and "Airline" not in raw.columns:
        raw = raw.with_columns(col("Reporting_Airline").alias("Airline"))

    select_exprs = []
    for column in FINAL_COLUMNS:
        if column in raw.columns:
            select_exprs.append(col(column))
        else:
            select_exprs.append(pl.lit(None).alias(column))

    normalised = raw.select(select_exprs)
    casted = normalised.select(
        [col(name).cast(TYPE_MAP[name], strict=False).alias(name) for name in FINAL_COLUMNS]
    )
    return casted


def load_flight_data(data_dir: Path) -> pl.DataFrame:
    """Read and standardise all CSVs in *data_dir* into a single Polars frame."""

    frames: List[pl.DataFrame] = []
    failures: List[Tuple[str, str]] = []
    for csv_path in tqdm(discover_csv_files(data_dir), desc="Loading CSVs", unit="file"):
        try:
            frames.append(read_and_normalize_csv(csv_path))
        except Exception as exc:  # pragma: no cover - defensive logging
            failures.append((csv_path.name, str(exc)))

    if not frames:
        raise RuntimeError("All CSV loads failed; cannot continue.")

    if failures:
        print("Warning: failed to load the following files:")
        for name, err in failures:
            print(f"  - {name}: {err}")

    combined = pl.concat(frames, how="vertical_relaxed")
    print(
        f"Loaded {len(frames)} files containing {combined.height:,} rows and {combined.width} columns"
    )
    return combined


def augment_flight_features(df: pl.DataFrame) -> pl.DataFrame:
    """Derive helper columns required for aggregation and analytics."""

    parsed = df.with_columns(
        col("FlightDate").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("FlightDate")
    )

    enriched = parsed.with_columns(
        col("FlightDate").dt.month().alias("Month"),
        (col("CRSDepTime") // 100).alias("Hour"),
    )

    cleaned_strings = enriched.with_columns(
        [
            pl.when(col(name).is_null() | (col(name) == ""))
            .then(pl.lit(None))
            .otherwise(col(name))
            .alias(name)
            for name in STRING_NULL_COLUMNS
        ]
    )

    feature_ready = cleaned_strings.with_columns(
        (col("DepDelay") <= 15).alias("OnTimeFlag"),
        (col("DepDelay") >= 60).alias("SevereDelayFlag"),
        pl.when(col("DepDelay") <= 0)
        .then(pl.lit(0.0))
        .otherwise(col("DepDelay"))
        .alias("PositiveDepDelay"),
    )
    return feature_ready


def summarise_groups(
    df: pl.DataFrame,
    group_columns: Sequence[str],
    min_records: int,
) -> pl.DataFrame:
    """Aggregate delay metrics for the provided grouping columns."""

    summary = (
        df.group_by(group_columns)
        .agg(
            col("OnTimeFlag").mean().alias("pLessThan15"),
            col("SevereDelayFlag").mean().alias("pGreaterThan60"),
            col("PositiveDepDelay").quantile(0.9).alias("delay90th"),
            col("PositiveDepDelay").median().alias("delayMean"),
            col("Cancelled").mean().alias("pCancel"),
            col("PositiveDepDelay").count().alias("n"),
            col("ArrDelay").mean().alias("arrDelayMean"),
            col("ArrDelay").std().alias("arrDelayStd"),
            col("PositiveDepDelay").std().alias("delayStd"),
        )
        .filter(col("n") >= min_records)
    )
    return summary


def infer_spikiness(delay_std: float) -> str:
    """Map the delay standard deviation to a qualitative spikiness bucket."""

    if delay_std < 10:
        return "low"
    if delay_std < 20:
        return "medium"
    return "high"


def fit_weibull(
    mass_under_15: float,
    mass_over_60: float,
    spikiness: str,
    pseudo_mean: float,
) -> Tuple[float, float]:
    """Fit a Weibull distribution matching key mass constraints."""

    spikiness_map = {"low": 1.5, "medium": 2.5, "high": 4.0}
    preferred_shape = spikiness_map.get(spikiness, 2.5)

    def objective(params: Sequence[float]) -> float:
        k, lam = params
        if k <= 0 or lam <= 0:
            return 1e6

        dist = weibull_min(c=k, scale=lam)
        mass_15 = dist.cdf(15)
        mass_60 = 1 - dist.cdf(60)

        if k > 1:
            actual_mode = lam * ((k - 1) / k) ** (1 / k)
            mode_error = (actual_mode - pseudo_mean) ** 2
        else:
            mode_error = pseudo_mean**2

        shape_preference = (k - preferred_shape) ** 2

        return (
            10.0 * (mass_15 - mass_under_15) ** 2
            + 10.0 * (mass_60 - mass_over_60) ** 2
            + 0.1 * mode_error
            + 0.01 * shape_preference
        )

    starting_points = (
        (preferred_shape, 10.0),
        (preferred_shape * 0.7, 15.0),
        (preferred_shape * 1.3, 8.0),
        (2.0, 12.0),
    )

    best_params = None
    best_error = np.inf
    for start in starting_points:
        try:
            result = minimize(
                objective,
                x0=start,
                bounds=WEIBULL_BOUNDS,
                method="L-BFGS-B",
            )
        except Exception:  # pragma: no cover - defensive
            continue

        if result.success and result.fun < best_error:
            best_error = result.fun
            best_params = result.x

    if best_params is None:
        raise ValueError("Weibull optimisation failed to converge")

    return float(best_params[0]), float(best_params[1])


def fit_single_weibull(args: Tuple[float, float, float, float]) -> Tuple[float, float]:
    """Run a single Weibull fit with guard rails for parallel execution."""

    mass_under_15, mass_over_60, pseudo_mean, delay_std = args
    try:
        spikiness = infer_spikiness(delay_std)
        return fit_weibull(mass_under_15, mass_over_60, spikiness, pseudo_mean)
    except Exception:  # pragma: no cover - fall back to safe defaults
        return DEFAULT_WEIBULL_RESULT


def fit_weibull_parallel_joblib(
    mass_under_15: Iterable[float],
    mass_over_60: Iterable[float],
    delay_mean: Iterable[float],
    delay_std: Iterable[float],
    n_cores: int | None,
) -> Tuple[List[float], List[float]]:
    """Parallelise Weibull fits with joblib while tracking progress."""

    args_list = list(zip(mass_under_15, mass_over_60, delay_mean, delay_std))
    if not args_list:
        return [], []

    if n_cores is None:
        n_cores = max(1, mp.cpu_count() - 1)

    chunk_size = max(1, len(args_list) // (n_cores * 4) or 1)
    results: List[Tuple[float, float]] = []

    with tqdm(total=len(args_list), desc="Fitting Weibull", unit="group") as progress:
        for start in range(0, len(args_list), chunk_size):
            chunk = args_list[start : start + chunk_size]
            chunk_results = Parallel(n_jobs=n_cores)(
                delayed(fit_single_weibull)(arguments) for arguments in chunk
            )
            results.extend(chunk_results)
            progress.update(len(chunk))

    shapes, scales = zip(*results) if results else ([], [])
    return list(shapes), list(scales)


def build_chrome_datasets(
    df: pl.DataFrame,
    min_records: int,
    n_cores: int | None,
) -> dict[int, pl.DataFrame]:
    """Create the chrome parquet datasets for each grouping depth."""

    chrome: dict[int, pl.DataFrame] = {}
    for level in CHROME_LEVELS:
        group_cols = GROUP_COLUMNS[:level]
        summary = summarise_groups(df, group_cols, min_records)
        if summary.is_empty():
            print(f"No groups met the minimum threshold for n={level}; skipping.")
            chrome[level] = summary
            continue

        shapes, scales = fit_weibull_parallel_joblib(
            summary["pLessThan15"].to_list(),
            summary["pGreaterThan60"].to_list(),
            summary["delayMean"].to_list(),
            summary["delayStd"].to_list(),
            n_cores=n_cores,
        )

        chrome[level] = summary.with_columns(
            pl.Series("shape", shapes),
            pl.Series("scale", scales),
        )

        print(
            f"Prepared chrome{level} with {chrome[level].height:,} groups (min records: {min_records})"
        )

    return chrome


def write_outputs(
    df: pl.DataFrame,
    chrome: dict[int, pl.DataFrame],
    output_dir: Path,
    overwrite: bool,
) -> None:
    """Persist parquet and pickle artefacts to the target directory."""

    output_dir.mkdir(parents=True, exist_ok=True)

    data_big_path = output_dir / DEFAULT_PARQUET
    if data_big_path.exists() and not overwrite:
        print(f"Skipping overwrite of existing {data_big_path}")
    else:
        df.write_parquet(data_big_path)
        print(f"Wrote consolidated dataset to {data_big_path}")

    for level, dataset in chrome.items():
        parquet_path = output_dir / f"chrome{level}.parquet"
        if parquet_path.exists() and not overwrite:
            print(f"Skipping overwrite of existing {parquet_path}")
            continue
        dataset.write_parquet(parquet_path)
        print(f"Wrote chrome parquet for n={level} to {parquet_path}")

    pickle_path = output_dir / CHROME_PICKLE
    with pickle_path.open("wb") as handle:
        pickle.dump(chrome, handle, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"Serialised all chrome datasets to {pickle_path}")


def main() -> None:
    args = parse_args()

    print("Step 1/4: Loading raw CSV extracts…")
    base_df = load_flight_data(args.data_dir)

    print("Step 2/4: Engineering derived features…")
    enriched_df = augment_flight_features(base_df)

    print("Step 3/4: Building chrome aggregates and Weibull fits…")
    chrome = build_chrome_datasets(enriched_df, args.min_records, args.n_cores)

    print("Step 4/4: Writing parquet and pickle outputs…")
    write_outputs(enriched_df, chrome, args.output_dir, args.overwrite)

    print("Done. Run time will scale with input size and available CPU cores.")


if __name__ == "__main__":
    main()
