#!/usr/bin/env python3
"""
VCF Genotype Sampler

Samples genotype values from a VCF file for validation purposes.
Outputs a CSV with up to MAX_SAMPLES_PER_GENOTYPE occurrences of each distinct genotype.
"""

import pandas as pd
import sys
import argparse
import collections
import random
from typing import Dict, List, Tuple

# Configuration
MAX_SAMPLES_PER_GENOTYPE = 10
ROW_SAMPLE_RATE = 0.1  # Sample 10% of rows for efficiency
CHUNK_SIZE = 100  # Process this many rows at a time


def get_vcf_info(vcf_path: str) -> Tuple[int, List[str]]:
    """Get VCF header info without loading the full file"""
    with open(vcf_path, "r") as f:
        for line_num, line in enumerate(f):
            if line.startswith("##"):
                continue
            elif line.startswith("#CHROM"):
                # This is the header line
                headers = line.strip().split("\t")
                format_idx = headers.index("FORMAT")
                sample_names = headers[format_idx + 1 :]
                return line_num, headers, sample_names
            else:
                raise ValueError("No #CHROM header line found in VCF file")

    raise ValueError("VCF file appears to be empty or malformed")


def process_vcf_in_chunks(
    vcf_path: str, used_1_based_coordinate:bool, sample_rate: float = ROW_SAMPLE_RATE, chunk_size: int = CHUNK_SIZE, 
) -> Tuple[Dict, collections.Counter, int, int]:
    """Process VCF file in chunks to avoid memory issues"""

    print(f"Loading VCF file: {vcf_path}", file=sys.stderr)

    # Get header info
    header_line_num, headers, sample_names = get_vcf_info(vcf_path)

    print(f"Found {len(sample_names)} samples", file=sys.stderr)
    print(
        f"Sample names: {sample_names[:5]}{'...' if len(sample_names) > 5 else ''}",
        file=sys.stderr,
    )

    genotype_samples = collections.defaultdict(list)
    genotype_counts = collections.Counter()
    total_genotypes = 0
    nocall_count = 0
    rows_processed = 0

    print(f"Processing file in chunks of {chunk_size} rows...", file=sys.stderr)

    # Read file in chunks
    chunk_reader = pd.read_csv(
        vcf_path,
        sep="\t",
        skiprows=header_line_num + 1,
        names=headers,
        chunksize=chunk_size,
        low_memory=False,
        dtype={"#CHROM": str, "ALT": str},
    )

    for chunk_num, chunk_df in enumerate(chunk_reader):
        # Apply row sampling within each chunk
        if sample_rate < 1.0:
            chunk_df = chunk_df.sample(
                frac=sample_rate, random_state=42 + chunk_num
            ).reset_index(drop=True)
            if chunk_num == 0:  # Only log this once
                print(
                    f"Sampling {sample_rate * 100:.1f}% of rows within each chunk",
                    file=sys.stderr,
                )

        # Process this chunk
        chunk_genotypes, chunk_counts, chunk_total, chunk_nocalls = process_chunk(
            chunk_df, sample_names, genotype_samples, 
            used_1_based_coordinate = used_1_based_coordinate
        )

        # Update global counters
        genotype_counts.update(chunk_counts)
        total_genotypes += chunk_total
        nocall_count += chunk_nocalls
        rows_processed += len(chunk_df)

        # Progress reporting
        if (chunk_num + 1) % 10 == 0:
            print(
                f"Processed {chunk_num + 1} chunks ({rows_processed:,} rows)",
                file=sys.stderr,
            )

        # Early exit if we have enough samples for all common genotypes
        if chunk_num > 50 and all(
            len(samples) >= MAX_SAMPLES_PER_GENOTYPE
            for genotype, samples in genotype_samples.items()
            if genotype_counts[genotype] > 10
        ):
            print(
                "Early exit: sufficient samples collected for common genotypes",
                file=sys.stderr,
            )
            break

    print(
        f"Completed processing: {rows_processed:,} rows in {chunk_num + 1} chunks",
        file=sys.stderr,
    )

    return genotype_samples, genotype_counts, total_genotypes, nocall_count


def process_chunk(
    chunk_df: pd.DataFrame, sample_names: List[str], genotype_samples: Dict,
    used_1_based_coordinate = False
) -> Tuple[Dict, collections.Counter, int, int]:
    """Process a single chunk of the VCF file"""

    chunk_counts = collections.Counter()
    chunk_total = 0
    chunk_nocalls = 0

    for idx, row in chunk_df.iterrows():
        reference_name = row["#CHROM"]
        start_position = row["POS"]

        # Process each sample for this variant
        for sample_name in sample_names:
            # Skip if we already have enough samples for any genotype
            sample_value = row[sample_name]
            genotype_str = extract_genotype_from_sample(sample_value)

            chunk_total += 1
            chunk_counts[genotype_str] += 1

            # Track no-calls
            if genotype_str in ["./.", ".|.", "."]:
                chunk_nocalls += 1

            # Only collect if we don't have enough samples yet
            if len(genotype_samples[genotype_str]) < MAX_SAMPLES_PER_GENOTYPE:
                genotype_samples[genotype_str].append(
                    {
                        "reference_name": reference_name,
                        "start_position": start_position - (int(not used_1_based_coordinate)),
                        "sample_name": sample_name,
                        "genotype": genotype_str,
                    }
                )

    return genotype_samples, chunk_counts, chunk_total, chunk_nocalls


def extract_genotype_from_sample(sample_value) -> str:
    """Extract genotype from sample column (before first colon if FORMAT data present)"""
    if pd.isna(sample_value):
        return "./."

    # Convert to string first in case pandas read it as int/float
    sample_str = str(sample_value)

    # Sample format is typically GT:DP:GQ:... where GT is the genotype
    # We want just the GT part
    return sample_str.split(":")[0]


def create_output_dataframe(
    genotype_samples: Dict,
    genotype_counts: collections.Counter,
    total_genotypes: int,
    nocall_count: int,
) -> pd.DataFrame:
    """Convert sampled genotypes to output DataFrame"""

    all_samples = []
    for genotype_str, samples in genotype_samples.items():
        all_samples.extend(samples)

    # Sort by chromosome and position for consistent output
    all_samples.sort(
        key=lambda x: (x["reference_name"], x["start_position"], x["sample_name"])
    )

    df = pd.DataFrame(all_samples)

    # Summary statistics
    unique_genotypes = len(genotype_counts)
    nocall_pct = 100 * nocall_count / total_genotypes if total_genotypes > 0 else 0

    print("\nSampling complete:", file=sys.stderr)
    print(f"Total genotypes processed: {total_genotypes:,}", file=sys.stderr)
    print(f"Unique genotype values: {unique_genotypes}", file=sys.stderr)
    print(f"No-call rate: {nocall_pct:.1f}% ({nocall_count:,})", file=sys.stderr)

    print("\nGenotype frequency summary:", file=sys.stderr)
    for genotype, count in genotype_counts.most_common(10):
        samples_collected = len(genotype_samples[genotype])
        print(
            f"  {genotype}: {count:,} occurrences, {samples_collected} samples collected",
            file=sys.stderr,
        )

    if unique_genotypes > 10:
        print(
            f"  ... and {unique_genotypes - 10} more genotype values", file=sys.stderr
        )

    print("\nOutput summary:", file=sys.stderr)
    print(f"Total samples in output: {len(df)}", file=sys.stderr)
    print(f"Genotypes represented: {df['genotype'].nunique()}", file=sys.stderr)
    print(f"Chromosomes covered: {df['reference_name'].nunique()}", file=sys.stderr)
    print(f"Samples covered: {df['sample_name'].nunique()}", file=sys.stderr)

    return df


def main():
    global MAX_SAMPLES_PER_GENOTYPE

    parser = argparse.ArgumentParser(
        description="Sample genotype values from VCF file for validation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage - output to stdout
  python genotype_sampler.py input.vcf > samples.csv
  
  # Save to specific file
  python genotype_sampler.py input.vcf --output samples.csv
  
  # Process full file (no row sampling)
  python genotype_sampler.py input.vcf --full-file
  
  # Adjust sample count per genotype
  python genotype_sampler.py input.vcf --max-per-genotype 20

Output CSV columns:
  reference_name  - Chromosome/contig name
  start_position  - Variant position
  sample_name     - Sample identifier
  genotype        - Genotype string exactly as in VCF
        """,
    )

    parser.add_argument("vcf_file", help="Input VCF file path")

    parser.add_argument("--output", "-o", help="Output CSV file (default: stdout)")

    parser.add_argument(
        "--max-per-genotype",
        type=int,
        default=MAX_SAMPLES_PER_GENOTYPE,
        help=f"Maximum samples to collect per genotype value (default: {MAX_SAMPLES_PER_GENOTYPE})",
    )

    parser.add_argument(
        "--sample-rate",
        type=float,
        default=ROW_SAMPLE_RATE,
        help=f"Fraction of rows to sample for efficiency (default: {ROW_SAMPLE_RATE})",
    )

    parser.add_argument(
        "--used-1-based-coordinate",
        action="store_true",
        help="This flag indicated that the data in BigQuery is 1-based coordinate"
    )

    parser.add_argument(
        "--full-file",
        action="store_true",
        help="Process entire file without row sampling",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE,
        help=f"Number of rows to process at once (default: {CHUNK_SIZE})",
    )

    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducible sampling"
    )

    args = parser.parse_args()

    # Set global constants from arguments
    MAX_SAMPLES_PER_GENOTYPE = args.max_per_genotype

    # Set random seed
    random.seed(args.seed)

    # Determine sampling rate
    sample_rate = 1.0 if args.full_file else args.sample_rate

    print("=" * 60, file=sys.stderr)
    print("VCF GENOTYPE SAMPLER", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Input file: {args.vcf_file}", file=sys.stderr)
    print(f"Max samples per genotype: {args.max_per_genotype}", file=sys.stderr)
    print(f"Row sampling rate: {sample_rate * 100:.1f}%", file=sys.stderr)
    print(f"Chunk size: {args.chunk_size}", file=sys.stderr)
    print(f"Random seed: {args.seed}", file=sys.stderr)
    print(f"Output: {'stdout' if not args.output else args.output}", file=sys.stderr)
    print("", file=sys.stderr)

    # Process VCF data in chunks
    genotype_samples, genotype_counts, total_genotypes, nocall_count = (
        process_vcf_in_chunks(args.vcf_file, args.used_1_based_coordinate, sample_rate, args.chunk_size)
    )

    # Create output DataFrame
    output_df = create_output_dataframe(
        genotype_samples, genotype_counts, total_genotypes, nocall_count
    )

    # Write output
    if args.output:
        output_df.to_csv(args.output, index=False)
        print(f"\nResults written to: {args.output}", file=sys.stderr)
    else:
        output_df.to_csv(sys.stdout, index=False)

    print("\n✅ Sampling completed successfully", file=sys.stderr)


if __name__ == "__main__":
    main()
