#!/usr/bin/env python

import argparse
import os
import subprocess
import csv
import glob
import json


def sanitize_name(name):
    """Replace invalid characters in strain names with dashes."""
    return "".join(c if c.isalnum() else "-" for c in name)


def process_tsv(input_file):
    """Process the TSV file and group genome IDs by strain."""
    strain_data = {}
    with open(input_file, newline='', encoding="utf-8") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            genome_id = row['genome.genome_id']
            segment = row['genome.segment']
            strain = row['genome.strain']
            sanitized_strain = sanitize_name(strain)

            if sanitized_strain not in strain_data:
                strain_data[sanitized_strain] = []

            strain_data[sanitized_strain].append((genome_id, strain, segment))
    return strain_data


def fetch_h5n1_genomes(output_tsv, date):
    """Download new genomes, segments and strain data."""

    query_date = f"{date}T00:00:00.000Z"
    command = [
        "p3-all-genomes",
        "--eq", "subtype,H5N1",
        "--gt", f"date_inserted,{query_date}",
        "--attr", "segment",
        "--attr", "strain"
    ]

    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)

        # Write output to file
        with open(output_tsv, "w", encoding="utf-8") as outfile:
            outfile.write(result.stdout)

        total_ids = result.stdout.strip().count("\n")

        print(f"Genome data saved to {output_tsv}")
        print(f"Total genome IDs fetched: {total_ids}")
    except subprocess.CalledProcessError as e:
        print(f"Error fetching genomes: {e}")


def download_and_save_fasta(strain_data, failed_strains_file, work_dir):
    """Download FASTA sequences, reformat headers, and save to strain-specific files."""
    failed_strains = []  # Keep track of strains that don't have all 8 segments
    genotype_results = {}

    for sanitized_strain, entries in strain_data.items():
        # Group by segment and check if all 8 segments are present
        segment_count = len(set(segment for _, _, segment in entries))  # Count unique segments

        if segment_count == 8:  # Process only if all 8 segments are present
            # Sort entries by segment to ensure correct order
            entries.sort(key=lambda x: x[2])  # Sort by segment
            fasta_name = f"{sanitized_strain}.fasta"
            fasta_file = os.path.join(work_dir, fasta_name)

            with open(fasta_file, 'w', encoding='utf-8') as fasta_out:
                for genome_id, strain, segment in entries:
                    print(f"Processing: genome_id={genome_id}, strain={strain}, segment={segment}")
                    try:
                        # Run p3-genome-fasta to get the FASTA sequence
                        fasta_data = subprocess.run(
                            ["p3-genome-fasta", genome_id],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                            check=True
                        ).stdout

                        # Process and reformat the FASTA header
                        lines = fasta_data.splitlines()
                        for i, line in enumerate(lines):
                            if line.startswith(">"):
                                lines[i] = f">{genome_id} {strain} {segment}"  # Replace header
                        reformatted_fasta = "\n".join(lines)

                        # Write the reformatted FASTA to the output file
                        fasta_out.write(reformatted_fasta + "\n")
                    except subprocess.CalledProcessError as e:
                        print(f"Error processing genome_id={genome_id}: {e.stderr}")

            try:
                print(f"Running genoflu.py for {fasta_file}")
                subprocess.run(
                    ["/home/ac.mkuscuog/git/GenoFLU/bin/genoflu.py", "-f", fasta_name],
                    check=True,
                    cwd=work_dir
                )

                # Locate the output TSV file with a wildcard pattern
                tsv_files = glob.glob(os.path.join(work_dir, f"{sanitized_strain}_*_stats.tsv"))
                if tsv_files:
                    genoflu_tsv_file = tsv_files[0]  # Assume the first match is the correct one
                    print(f"Found genoflu result: {genoflu_tsv_file}")

                    # Extract the genotype from the genoflu output
                    with open(genoflu_tsv_file, newline='', encoding='utf-8') as genoflu_out:
                        genoflu_reader = csv.DictReader(genoflu_out, delimiter='\t')
                        for row in genoflu_reader:
                            genotype_results[sanitized_strain] = row['Genotype']
                            break  # Process only the first line of the result
                else:
                    print(f"No stats file found for strain: {sanitized_strain}")
            except subprocess.CalledProcessError as e:
                print(f"Error running genoflu.py for {fasta_file}: {e.stderr}")
        else:
            failed_strains.append(sanitized_strain)  # Add to failed strains list

    # Write the failed strains to the log file
    if failed_strains:
        with open(failed_strains_file, 'w', encoding='utf-8') as f:
            for strain in failed_strains:
                f.write(strain + "\n")
        print(f"Some strains didn't have all 8 segments. Check {failed_strains_file} for details.")
    else:
        print("All strains have all 8 segments!")

    return genotype_results


def add_genotype_to_json(input_file, genotype_results, output_file):
    try:
        genome_data = []

        with open(input_file, newline='', encoding="utf-8") as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t')

            # Validate necessary columns
            required_columns = {"genome.strain", "genome.genome_id"}
            if not required_columns.issubset(reader.fieldnames):
                missing = required_columns - set(reader.fieldnames)
                print(f"Error: Missing required columns in input TSV: {missing}")
                return False

            missing_strains = 0

            for row in reader:
                strain = sanitize_name(row['genome.strain'])
                subclade = genotype_results.get(strain)

                if subclade and not subclade.startswith("Not assigned"):
                    genome_data.append({
                        "genome_id": row['genome.genome_id'],
                        "subclade": {"set": subclade}
                    })
                else:
                    missing_strains += 1

            # Write to JSON file
            with open(output_file, "w", encoding="utf-8") as json_file:
                json.dump(genome_data, json_file, indent=4)

            print(f"Processed {len(genome_data)} entries, {missing_strains} strains had no valid subclade.")
            return True

    except Exception as e:
        print(f"Error processing TSV to JSON: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Genotyper Script")
    parser.add_argument("--date", required=True, help="Run date (YYYY-MM-DD)")
    parser.add_argument("--work_dir", required=True, help="Working directory")
    parser.add_argument("--update_file", required=True, help="Output update file")

    args = parser.parse_args()
    work_dir = args.work_dir

    genome_file = os.path.join(work_dir, "genomes.tbl")
    genotypes_file = args.update_file if args.update_file else os.path.join(work_dir, "genotypes.tsv")
    failed_strains_file = os.path.join(work_dir, "failed_strains.txt")

    fetch_h5n1_genomes(genome_file, args.date)
    strain_data = process_tsv(genome_file)
    if strain_data:
        genotype_results = download_and_save_fasta(strain_data, failed_strains_file, work_dir)
        add_genotype_to_json(genome_file, genotype_results, genotypes_file)

        print(f"Final output with genotypes saved to {genotypes_file}")
    else:
        print(f"There is no new data to be processed")
