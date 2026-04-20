from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def _looks_like_uri(path: str) -> bool:
    return "://" in path


def _windows_short_path(path: str) -> str:
    if os.name != "nt":
        return path
    try:
        import ctypes

        buf = ctypes.create_unicode_buffer(4096)
        rc = ctypes.windll.kernel32.GetShortPathNameW(str(path), buf, len(buf))
        return buf.value if rc else path
    except Exception:
        return path


def _shorten_if_under_base(path: str, base_dir: Path) -> str:
    if os.name != "nt" or _looks_like_uri(path):
        return path
    try:
        p = Path(path).resolve()
        base = base_dir.resolve()
        rel = p.relative_to(base)
    except Exception:
        # If it isn't under base (or can't be resolved), just return as-is.
        return path

    base_short = _windows_short_path(str(base))
    return str(Path(base_short) / rel)


def _configure_windows_hadoop() -> None:
    if os.name != "nt":
        return

    # Spark on Windows may require winutils.exe (HADOOP_HOME / hadoop.home.dir) to write outputs.
    if os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir"):
        return

    script_dir = Path(__file__).resolve().parent
    for base in [script_dir, *script_dir.parents]:
        winutils = base / "hadoop" / "bin" / "winutils.exe"
        if winutils.exists():
            hadoop_home = winutils.parents[1]  # .../hadoop
            os.environ["HADOOP_HOME"] = str(hadoop_home)
            os.environ["hadoop.home.dir"] = str(hadoop_home)
            os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ.get("PATH", "")
            return


def _find_repo_hadoop_home() -> Path | None:
    script_dir = Path(__file__).resolve().parent
    for base in [script_dir, *script_dir.parents]:
        winutils = base / "hadoop" / "bin" / "winutils.exe"
        if winutils.exists():
            return winutils.parents[1]  # .../hadoop
    return None


def _resolve_path(value: str, base_dir: Path) -> str:
    if _looks_like_uri(value):
        return value
    p = Path(value).expanduser()
    if not p.is_absolute():
        p = base_dir / p
    return str(p.resolve())


def _year_columns(columns: list[str], start: int = 1991, end: int = 2025) -> list[str]:
    years = []
    for col in columns:
        m = re.fullmatch(r"Y(\d{4})", col)
        if not m:
            continue
        year = int(m.group(1))
        if start <= year <= end:
            years.append((year, col))
    return [c for _, c in sorted(years)]


def main() -> None:
    parser = argparse.ArgumentParser(description="FAOSTAT prices: USD-only + unpivot to tidy parquet.")
    parser.add_argument(
        "--input",
        default=os.environ.get("FAOSTAT_INPUT", "data/raw/faostat_prices.csv"),
        help="Input CSV path (local path or URI like gs://...). Env: FAOSTAT_INPUT.",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("FAOSTAT_OUTPUT", "data/processed/faostat_clean"),
        help="Output parquet directory (local path or URI like gs://...). Env: FAOSTAT_OUTPUT.",
    )
    args = parser.parse_args()

    base_dir = Path.cwd()
    input_path = _resolve_path(args.input, base_dir)
    output_path = _resolve_path(args.output, base_dir)

    if not _looks_like_uri(input_path) and not Path(input_path).exists():
        raise FileNotFoundError(f"Missing input CSV: {input_path}")

    _configure_windows_hadoop()

    # Reduce Windows path length issues by using 8.3 short paths for repo-local files.
    input_path = _shorten_if_under_base(input_path, base_dir)
    output_path = _shorten_if_under_base(output_path, base_dir)

    spark = (
        SparkSession.builder.appName("faostat_transform")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )

    # When running via `spark-submit` on Windows, the JVM is already started before this script,
    # so setting env vars above may not be enough. Set the JVM system property too.
    if os.name == "nt":
        hadoop_home = _find_repo_hadoop_home()
        if hadoop_home is not None:
            current = spark._jvm.java.lang.System.getProperty("hadoop.home.dir")
            if not current:
                spark._jvm.java.lang.System.setProperty("hadoop.home.dir", str(hadoop_home))
                # If Hadoop Shell already initialized before, this won't help, but setting it is harmless.

    df = (
        spark.read.option("header", True)
        .option("multiLine", False)
        .option("escape", '"')
        .csv(input_path)
    )

    required = {"Area", "Item"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            "Missing required columns: "
            + ", ".join(missing)
            + ". Available columns: "
            + ", ".join(df.columns)
        )

    # Standardize for cross-country comparability: keep USD only.
    if "Unit" in df.columns:
        df = df.filter(F.trim(F.col("Unit")) == F.lit("USD"))
    elif "Element" in df.columns:
        df = df.filter(F.col("Element").contains("USD"))
    else:
        raise ValueError(
            "To filter USD-only, expected a 'Unit' or 'Element' column. "
            f"Available columns: {', '.join(df.columns)}"
        )

    year_cols = _year_columns(df.columns, start=1991, end=2025)
    if not year_cols:
        raise ValueError(
            "No year columns found matching Y1991..Y2025. "
            f"Available columns: {', '.join(df.columns)}"
        )

    base = df.select(F.col("Area").alias("country"), F.col("Item").alias("product"), *year_cols)

    # Unpivot wide year columns into rows: (year, price)
    stacked = ", ".join([f"'{c[1:]}', `{c}`" for c in year_cols])  # '1991', `Y1991`, ...
    long_df = base.select(
        "country",
        "product",
        F.expr(f"stack({len(year_cols)}, {stacked}) as (year, price)"),
    )

    cleaned = (
        long_df.withColumn("year", F.col("year").cast("int"))
        .withColumn(
            "price",
            F.regexp_replace(F.trim(F.col("price").cast("string")), ",", "").cast("double"),
        )
        .filter(F.col("price").isNotNull())
        .select("country", "product", "year", F.col("price").cast("float").alias("price"))
    )

    cleaned.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
