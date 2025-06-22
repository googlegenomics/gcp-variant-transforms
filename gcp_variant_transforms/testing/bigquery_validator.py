#!/usr/bin/env python3
"""
VCF Ingestion Pipeline Validation Script

Compares sampled genotype data (from CSV) against BigQuery tables to validate
the Google Variant Transforms ingestion pipeline.
"""

from google.cloud import bigquery
import pandas as pd
from typing import Dict, List
import collections
import argparse
import sys


def setup_bigquery_client(project_id: str):
    """Initialize BigQuery client"""
    client = bigquery.Client(project=project_id)
    return client


def normalize_bigquery_genotype(genotype_list):
    """Convert BigQuery genotype format to normalized tuple"""
    if not genotype_list or len(genotype_list) == 0:
        return (None, None)

    # Handle various no-call representations
    if any(g is None or g == -1 or g == "." for g in genotype_list):
        return (None, None)

    # Convert to integers and sort
    try:
        int_genotypes = [int(g) for g in genotype_list]
        return tuple(sorted(int_genotypes))
    except (ValueError, TypeError):
        # Handle string genotypes like "./."
        if all(g == "." for g in genotype_list):
            return (None, None)
        return tuple(sorted(genotype_list))


def normalize_csv_genotype(genotype_str: str):
    """Convert CSV genotype string to normalized format for comparison"""
    if not genotype_str or genotype_str in ["./.", ".|.", "."]:
        return "./."

    # Return the genotype exactly as it appears in the CSV
    # This will be compared against the BigQuery genotype after conversion
    return genotype_str


def load_csv_samples(csv_path: str) -> Dict:
    """Load genotype samples from CSV file"""

    print(f"Loading genotype samples from: {csv_path}", file=sys.stderr)

    df = pd.read_csv(csv_path)

    print(f"Loaded {len(df)} genotype samples", file=sys.stderr)
    print(f"Columns: {list(df.columns)}", file=sys.stderr)

    # Validate expected columns
    expected_cols = ["reference_name", "start_position", "sample_name", "genotype"]
    missing_cols = set(expected_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing expected columns in CSV: {missing_cols}")

    # Group by genotype value
    genotype_coverage = collections.defaultdict(list)

    for _, row in df.iterrows():
        chrom = str(row["reference_name"])
        pos = int(row["start_position"])
        sample_name = str(row["sample_name"])
        genotype = normalize_csv_genotype(str(row["genotype"]))

        genotype_coverage[genotype].append((chrom, pos, sample_name))

    print(f"Found {len(genotype_coverage)} distinct genotype values:", file=sys.stderr)
    for genotype, locations in genotype_coverage.items():
        print(f"  {genotype}: {len(locations)} samples", file=sys.stderr)

    return {"genotype_coverage": dict(genotype_coverage), "total_samples": len(df)}


def get_sample_mapping(client: bigquery.Client, table_base_name: str) -> Dict[str, str]:
    """Fetch sample ID to sample name mapping from sample_info table"""

    sample_info_table = f"{table_base_name}__sample_info"

    query = f"""
    SELECT sample_id, sample_name
    FROM `{sample_info_table}`
    ORDER BY sample_id
    """

    print(f"Fetching sample mapping from {sample_info_table}...", file=sys.stderr)

    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()

        # Create bidirectional mapping
        id_to_name = dict(zip(df["sample_id"].astype(str), df["sample_name"]))
        name_to_id = dict(
            zip(df["sample_name"], df["sample_id"])
        )  # Keep sample_id as integer

        print(f"Found {len(id_to_name)} sample mappings", file=sys.stderr)
        print(
            f"Sample mapping examples: {dict(list(id_to_name.items())[:3])}",
            file=sys.stderr,
        )

        return {"id_to_name": id_to_name, "name_to_id": name_to_id}

    except Exception as e:
        print(f"Error fetching sample mapping: {e}", file=sys.stderr)
        print("Will attempt to proceed without sample mapping...", file=sys.stderr)
        return {"id_to_name": {}, "name_to_id": {}}


def build_validation_query(
    table_base_name: str,
    chrom: str,
    positions: List[int],
    sample_names: List[str] = None,
    sample_mapping: Dict = None,
):
    """Build SQL query for a specific chromosome table with proper sample mapping"""

    # Build the chromosome-specific table name
    table_name = f"{table_base_name}__{chrom}"
    sample_info_table = f"{table_base_name}__sample_info"

    # Create position filter for this chromosome
    if len(positions) == 1:
        position_filter = f"v.start_position = {positions[0]}"
    else:
        position_list = ", ".join(str(pos) for pos in positions)
        position_filter = f"v.start_position IN ({position_list})"

    # Create sample filter using sample names if provided
    sample_filter = ""
    if sample_names and sample_mapping and sample_mapping["name_to_id"]:
        # Convert sample names to BigQuery sample IDs
        bq_sample_ids = []
        for sample_name in sample_names:
            if sample_name in sample_mapping["name_to_id"]:
                bq_sample_ids.append(
                    sample_mapping["name_to_id"][sample_name]
                )  # Keep as integer
            else:
                print(
                    f"Warning: Sample {sample_name} not found in mapping",
                    file=sys.stderr,
                )

        if bq_sample_ids:
            sample_list = ", ".join(str(sid) for sid in bq_sample_ids)
            sample_filter = f"AND call.sample_id IN ({sample_list})"

    # Use JOIN to get sample names back - note: using sample_id instead of call_set_name
    query = f"""
    SELECT 
        v.reference_name,
        v.start_position,
        call.sample_id,
        s.sample_name,
        call.genotype as genotype
    FROM `{table_name}` v,
    UNNEST(call) as call
    LEFT JOIN `{sample_info_table}` s
    ON call.sample_id = s.sample_id
    WHERE {position_filter}
    {sample_filter}
    ORDER BY v.start_position, call.sample_id
    """

    return query


def fetch_bigquery_data(client: bigquery.Client, query: str) -> pd.DataFrame:
    """Execute BigQuery query and return results as DataFrame"""
    print("Executing BigQuery query...", file=sys.stderr)
    print(f"Query preview: {query[:200]}...", file=sys.stderr)

    query_job = client.query(query)
    df = query_job.to_dataframe()

    print(f"Retrieved {len(df)} rows from BigQuery", file=sys.stderr)
    return df


def compare_csv_bigquery(
    csv_samples: Dict,
    bq_project_id: str,
    bq_table_base_name: str,
    sample_subset: List[str] = None,
):
    """Compare CSV samples against BigQuery data across multiple chromosome tables"""

    client = setup_bigquery_client(bq_project_id)

    # First, get the sample mapping
    sample_mapping = get_sample_mapping(client, bq_table_base_name)

    # Group positions by chromosome and collect all sample names from CSV
    positions_by_chrom = collections.defaultdict(list)
    csv_lookup = {}  # (chrom, pos, sample_name) -> genotype
    all_csv_samples = set()  # Track all samples mentioned in CSV

    print("Preparing CSV data for comparison...", file=sys.stderr)
    for genotype, locations in csv_samples["genotype_coverage"].items():
        for chrom, pos, sample_name in locations:
            positions_by_chrom[chrom].append(pos)
            csv_lookup[(chrom, pos, sample_name)] = genotype
            all_csv_samples.add(sample_name)

    # Use sample subset if provided, otherwise use all samples from CSV
    samples_to_query = sample_subset if sample_subset else list(all_csv_samples)

    print(
        f"Will check {len(positions_by_chrom)} chromosomes with {len(csv_lookup)} total sample-position combinations",
        file=sys.stderr,
    )
    print(
        f"Filtering BigQuery to {len(samples_to_query)} specific samples",
        file=sys.stderr,
    )

    all_bq_data = []

    # Query each chromosome table separately
    for chrom, positions in positions_by_chrom.items():
        unique_positions = list(set(positions))  # Remove duplicates
        print(
            f"Querying chromosome {chrom}: {len(unique_positions)} unique positions",
            file=sys.stderr,
        )

        # Batch positions within chromosome to avoid query size limits
        batch_size = 100  # Can be larger since we're not crossing chromosomes
        position_batches = [
            unique_positions[i : i + batch_size]
            for i in range(0, len(unique_positions), batch_size)
        ]

        for batch_num, position_batch in enumerate(position_batches):
            if len(position_batches) > 1:
                print(
                    f"  Batch {batch_num + 1}/{len(position_batches)} for {chrom}",
                    file=sys.stderr,
                )

            try:
                # Build and execute query for this chromosome batch
                # Pass the samples we want to filter to
                query = build_validation_query(
                    bq_table_base_name,
                    chrom,
                    position_batch,
                    samples_to_query,
                    sample_mapping,
                )
                batch_df = fetch_bigquery_data(client, query)

                # Add chromosome info to the dataframe if not present
                if "reference_name" not in batch_df.columns:
                    batch_df["reference_name"] = chrom

                all_bq_data.append(batch_df)

            except Exception as e:
                print(
                    f"Error querying {chrom} positions {position_batch}: {e}",
                    file=sys.stderr,
                )
                # Continue with other chromosomes
                continue

    # Combine all results
    if all_bq_data:
        bq_df = pd.concat(all_bq_data, ignore_index=True)
    else:
        print("No data returned from BigQuery!", file=sys.stderr)
        return None

    print(f"Total BigQuery records retrieved: {len(bq_df)}", file=sys.stderr)

    # Perform comparison
    return validate_data_consistency(csv_lookup, bq_df)


def convert_bigquery_genotype_to_string(genotype_list) -> str:
    """Convert BigQuery genotype array back to VCF-style string for comparison"""
    # Handle None or empty cases
    if genotype_list is None:
        return "./."

    # Convert to list if it's a numpy array
    if hasattr(genotype_list, "tolist"):
        genotype_list = genotype_list.tolist()

    # Check if empty
    if len(genotype_list) == 0:
        return "./."

    # Handle no-call representations
    if any(g is None or g == -1 or str(g) == "." for g in genotype_list):
        return "./."

    # Convert to strings and join with separator
    try:
        if len(genotype_list) == 1:
            # Haploid (e.g., chrY, chrX in males)
            return str(genotype_list[0])
        elif len(genotype_list) == 2:
            # Diploid
            return f"{genotype_list[0]}/{genotype_list[1]}"
        else:
            # Polyploid - join with /
            return "/".join(str(g) for g in genotype_list)
    except (ValueError, TypeError):
        return "./."


def validate_data_consistency(csv_lookup: Dict, bq_df: pd.DataFrame):
    """Compare CSV and BigQuery data to find discrepancies"""

    mismatches = []
    matches = []
    bq_missing = []
    csv_extra = []

    # Create BigQuery lookup using sample names from the JOIN
    bq_lookup = {}
    for _, row in bq_df.iterrows():
        chrom = str(row["reference_name"])
        pos = int(row["start_position"])
        sample_name = row["sample_name"]  # Now we have the original sample name

        if pd.isna(sample_name):
            print(
                f"Warning: Missing sample name for sample_id {row['sample_id']} at {chrom}:{pos}",
                file=sys.stderr,
            )
            continue

        bq_genotype = convert_bigquery_genotype_to_string(row["genotype"])

        bq_lookup[(chrom, pos, sample_name)] = bq_genotype

    print("Comparing CSV vs BigQuery data...", file=sys.stderr)
    print(
        f"CSV entries: {len(csv_lookup)}, BigQuery entries: {len(bq_lookup)}",
        file=sys.stderr,
    )

    # Check each CSV sample
    for (chrom, pos, sample_name), csv_genotype in csv_lookup.items():
        key = (chrom, pos, sample_name)

        if key in bq_lookup:
            bq_genotype = bq_lookup[key]
            if csv_genotype == bq_genotype:
                matches.append(
                    {
                        "chrom": chrom,
                        "pos": pos,
                        "sample": sample_name,
                        "genotype": csv_genotype,
                        "status": "MATCH",
                    }
                )
            else:
                mismatches.append(
                    {
                        "chrom": chrom,
                        "pos": pos,
                        "sample": sample_name,
                        "csv_genotype": csv_genotype,
                        "bq_genotype": bq_genotype,
                        "status": "MISMATCH",
                    }
                )
        else:
            bq_missing.append(
                {
                    "chrom": chrom,
                    "pos": pos,
                    "sample": sample_name,
                    "csv_genotype": csv_genotype,
                    "status": "MISSING_IN_BQ",
                }
            )

    # Check for extra BigQuery data
    for (chrom, pos, sample_name), bq_genotype in bq_lookup.items():
        if (chrom, pos, sample_name) not in csv_lookup:
            csv_extra.append(
                {
                    "chrom": chrom,
                    "pos": pos,
                    "sample": sample_name,
                    "bq_genotype": bq_genotype,
                    "status": "EXTRA_IN_BQ",
                }
            )

    # Summary report
    total_comparisons = len(csv_lookup)
    print("\n" + "=" * 60, file=sys.stderr)
    print("VALIDATION RESULTS", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Total comparisons: {total_comparisons}", file=sys.stderr)
    print(
        f"Matches: {len(matches)} ({len(matches) / total_comparisons * 100:.1f}%)",
        file=sys.stderr,
    )
    print(
        f"Mismatches: {len(mismatches)} ({len(mismatches) / total_comparisons * 100:.1f}%)",
        file=sys.stderr,
    )
    print(
        f"Missing in BigQuery: {len(bq_missing)} ({len(bq_missing) / total_comparisons * 100:.1f}%)",
        file=sys.stderr,
    )
    print(
        f"Extra in BigQuery: {len(csv_extra)} (expected from query structure)",
        file=sys.stderr,
    )

    if mismatches:
        print("\nFirst 10 MISMATCHES:", file=sys.stderr)
        for i, mismatch in enumerate(mismatches[:10]):
            print(
                f"  {mismatch['chrom']}:{mismatch['pos']} {mismatch['sample']}: "
                f"CSV={mismatch['csv_genotype']} vs BQ={mismatch['bq_genotype']}",
                file=sys.stderr,
            )

    if bq_missing:
        print("\nFirst 10 MISSING IN BIGQUERY:", file=sys.stderr)
        for i, missing in enumerate(bq_missing[:10]):
            print(
                f"  {missing['chrom']}:{missing['pos']} {missing['sample']}: "
                f"CSV={missing['csv_genotype']}",
                file=sys.stderr,
            )

    if csv_extra:
        print("\nFirst 5 EXTRA IN BIGQUERY:", file=sys.stderr)
        for i, extra in enumerate(csv_extra[:5]):
            print(
                f"  {extra['chrom']}:{extra['pos']} {extra['sample']}: "
                f"BQ={extra['bq_genotype']}",
                file=sys.stderr,
            )

    return {
        "matches": matches,
        "mismatches": mismatches,
        "bq_missing": bq_missing,
        "csv_extra": csv_extra,
        "summary": {
            "total_comparisons": total_comparisons,
            "match_rate": len(matches) / total_comparisons
            if total_comparisons > 0
            else 0,
            "mismatch_count": len(mismatches),
            "missing_count": len(bq_missing),
            "extra_count": len(csv_extra),
        },
    }


def generate_output_files(csv_filename: str, validation_results: Dict):
    """Generate output CSV files based on input filename"""
    import os

    # Get base filename without extension
    base_name = os.path.splitext(csv_filename)[0]

    # Create output filenames
    mismatch_file = f"{base_name}.bq_mismatch.csv"
    missing_file = f"{base_name}.bq_missing.csv"

    # Write mismatch results
    if validation_results["mismatches"]:
        mismatch_df = pd.DataFrame(validation_results["mismatches"])
        mismatch_df.to_csv(mismatch_file, index=False)
        print(
            f"Wrote {len(validation_results['mismatches'])} mismatches to: {mismatch_file}",
            file=sys.stderr,
        )
    else:
        print("No mismatches found - no mismatch file created", file=sys.stderr)

    # Write missing results
    if validation_results["bq_missing"]:
        missing_df = pd.DataFrame(validation_results["bq_missing"])
        missing_df.to_csv(missing_file, index=False)
        print(
            f"Wrote {len(validation_results['bq_missing'])} missing entries to: {missing_file}",
            file=sys.stderr,
        )
    else:
        print("No missing entries found - no missing file created", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate VCF ingestion pipeline by comparing genotype samples (from CSV) against BigQuery tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic validation
  python bigquery_validation.py samples.csv biovu-cloud-storage biovu-cloud-storage.vcf_test_import_2.chry10k
  
  # Limit to specific samples
  python bigquery_validation.py samples.csv my-project my-project.dataset.table_base --samples Person_1,Person_2,Person_3

Expected CSV format:
  reference_name,start_position,sample_name,genotype
  chr11,61489,Person_4857,0/1
  chr8,60209,Person_1,./.

Output files:
  input_name.bq_mismatch.csv - Genotype mismatches between CSV and BigQuery
  input_name.bq_missing.csv  - Entries missing in BigQuery

Note: The table_base_name should be the base name without chromosome suffix.
      For example, if your tables are named 'dataset.table__chr1', 'dataset.table__chr2', etc.,
      then use 'dataset.table' as the table_base_name.
        """,
    )

    parser.add_argument(
        "csv_file",
        help="Path to CSV file with genotype samples (from genotype_sampler.py)",
    )

    parser.add_argument(
        "bq_project_id", help='BigQuery project ID (e.g., "biovu-cloud-storage")'
    )

    parser.add_argument(
        "bq_table_base_name",
        help='BigQuery table base name without chromosome suffix (e.g., "project.dataset.table_base")',
    )

    parser.add_argument(
        "--samples",
        help="Comma-separated list of specific sample names to validate (optional)",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    args = parser.parse_args()

    # Parse sample list if provided
    sample_subset = None
    if args.samples:
        sample_subset = [s.strip() for s in args.samples.split(",")]
        print(f"Will validate only these samples: {sample_subset}", file=sys.stderr)

    print("=" * 60, file=sys.stderr)
    print("VCF INGESTION PIPELINE VALIDATION", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"CSV file: {args.csv_file}", file=sys.stderr)
    print(f"BigQuery project: {args.bq_project_id}", file=sys.stderr)
    print(f"Table base name: {args.bq_table_base_name}", file=sys.stderr)
    if sample_subset:
        print(f"Sample filter: {len(sample_subset)} specific samples", file=sys.stderr)
    print("", file=sys.stderr)

    # Load CSV data
    csv_samples = load_csv_samples(args.csv_file)

    print("\nQuerying BigQuery tables...", file=sys.stderr)
    print("Will query chromosome-specific tables like:", file=sys.stderr)
    print(f"  {args.bq_table_base_name}__chr1", file=sys.stderr)
    print(f"  {args.bq_table_base_name}__chrY", file=sys.stderr)
    print(f"  {args.bq_table_base_name}__sample_info", file=sys.stderr)
    print("", file=sys.stderr)

    validation_results = compare_csv_bigquery(
        csv_samples, args.bq_project_id, args.bq_table_base_name, sample_subset
    )

    if validation_results is None:
        print("❌ FAILED: Could not retrieve data from BigQuery", file=sys.stderr)
        sys.exit(1)

    # Final summary
    summary = validation_results["summary"]
    if summary["mismatch_count"] == 0 and summary["missing_count"] == 0:
        print(
            "\n🎉 SUCCESS: All sampled data matches between CSV and BigQuery!",
            file=sys.stderr,
        )
        print(
            "Your ingestion pipeline appears to be working correctly.", file=sys.stderr
        )
    else:
        print("\n⚠️  ISSUES FOUND:", file=sys.stderr)
        if summary["mismatch_count"] > 0:
            print(
                f"  - {summary['mismatch_count']} genotype mismatches", file=sys.stderr
            )
        if summary["missing_count"] > 0:
            print(
                f"  - {summary['missing_count']} entries missing in BigQuery",
                file=sys.stderr,
            )

        print(
            "\nThis suggests there may be bugs in your ingestion pipeline.",
            file=sys.stderr,
        )
        print(
            "Check the detailed output above for specific coordinates and samples.",
            file=sys.stderr,
        )

    # Show match rate
    match_rate = summary["match_rate"] * 100
    print(f"\nOverall match rate: {match_rate:.1f}%", file=sys.stderr)

    # Generate output CSV files
    generate_output_files(args.csv_file, validation_results)
