#!/usr/bin/env python3
"""Generates test input datasets with incremental sizes, prints expected sums,
and saves a summary CSV file.

Based on the C reference generator:
- Each number is random between 1 and 50 inclusive.
"""

import csv
import os
import random

MAX_NUM = 50
DATASETS = [
    ("test_10.txt", 10),
    ("test_100.txt", 100),
    ("test_1000.txt", 1_000),
    ("test_10000.txt", 10_000),
    ("test_100000.txt", 100_000),
]

OUTPUT_DIR = "test_data"
CSV_SUMMARY = os.path.join(OUTPUT_DIR, "expected_sums.csv")


def generate_files() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    summary_lines = []
    csv_rows = []

    for filename, count in DATASETS:
        filepath = os.path.join(OUTPUT_DIR, filename)
        total_sum = 0

        with open(filepath, "w", encoding="utf-8") as f:
            for _ in range(count):
                num = random.randint(1, MAX_NUM)
                f.write(f"{num}\n")
                total_sum += num

        size_kb = os.path.getsize(filepath) / 1024
        summary_lines.append(
            f"{filename:<18} | {count:>7} números | Soma esperada: {total_sum:>10} | Tamanho: {size_kb:.1f} KB"
        )
        csv_rows.append(
            {
                "filename": filename,
                "count": count,
                "expected_sum": total_sum,
                "size_kb": round(size_kb, 2),
            }
        )

    # Save summary CSV
    with open(CSV_SUMMARY, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["filename", "count", "expected_sum", "size_kb"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)

    print("\n" + "=" * 70)
    print("Arquivos de Teste Incrementais Gerados com Sucesso!")
    print("=" * 70)
    for line in summary_lines:
        print(line)
    print("=" * 70)
    print(f"Resumo salvo em CSV: {CSV_SUMMARY}\n")


if __name__ == "__main__":
    generate_files()
