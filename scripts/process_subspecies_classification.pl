#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long qw(GetOptions);
use File::Path qw(make_path);
use POSIX qw/strftime/;
use IPC::Run3 'run3';
use JSON;
use File::Copy qw(copy);

my $date;
my $work_dir;
my $update_file;

GetOptions(
    'date=s'        => \$date,
    'work_dir=s'    => \$work_dir,
    'update_file=s' => \$update_file,
) or die "Usage: $0 --date YYYY-MM-DD --work_dir DIR --update_file FILE\n";

# Validate required options
die "Missing required --date argument\n" unless defined $date;
die "Missing required --work_dir argument\n" unless defined $work_dir;
die "Missing required --update_file argument\n" unless defined $update_file;

my $download_dir = "$work_dir/download";
my $result_dir = "$work_dir/results/$date";
my $ws_path = "/mkuscuog\@bvbrc/home/subclass";

# Create output directory if it doesn't exist
make_path($work_dir) unless -d $work_dir;
make_path($download_dir) unless -d $download_dir;
make_path($result_dir) unless -d $result_dir;

print "Running ingestion for date: $date\n";
print "Working directory: $work_dir\n";
print "Update file: $update_file\n";

my %taxon_ids = (
    "dengue"    => "12637",
    "h1"        => "11320",
    "h1us"      => "11320",
    "h3"        => "11320",
    "h5"        => "11320",
    "monkeypox" => "10244"
);
my %types = (
    "dengue"    => "DENGUE",
    "h1"        => "SWINEH1",
    "h1us"      => "SWINEH1US",
    "h3"        => "SWINEH3",
    "h5"        => "INFLUENZAH5",
    "monkeypox" => "MPOX"
);
my %influenza_types = map {$_ => 1} qw(h1 h1us h3 h5);

my %clades = (
    "dengue"    => "subtype",
    "h1"        => "h1_clade_global",
    "h1us"      => "h1_clade_us",
    "h3"        => "h3_clade",
    "h5"        => "h5_clade",
    "monkeypox" => "clade"
);
my %attributes = (
    "dengue"    => "subtype",
    "h1"        => "segment",
    "h1us"      => "segment",
    "h3"        => "segment",
    "h5"        => "segment",
    "monkeypox" => "clade"
);
my %genome;

# Step 2: Download and process genomes
if (download_data($date)) {
    my %job_ids = submit_data();
    fetch_result(%job_ids);
    process_result();
}
else {
    print STDOUT "No new genome data found\n";
}

exit 0;

# Download genome data based on date
sub download_data {
    my $download_status;
    foreach my $type (keys %types) {
        print STDOUT "Downloading genomes ${type} ids.\n";

        my $id_file = "$download_dir/${type}_ids.txt";
        my $fasta_file = "$download_dir/${type}.fasta";
        unlink $fasta_file if -e $fasta_file; # Remove previous fasta file if it exists

        my @ids_command = (
            "p3-all-genomes",
            "--attr", "$attributes{$type}",
            "--attr", "biosample_accession",
            "--attr", "$clades{$type}",
            "--attr", "date_inserted",
            "--in", "taxon_lineage_ids,$taxon_ids{$type}",
            "--gt", "date_inserted,\"${date}T00:00:00Z\""
        );

        if ($type eq "h1us") {
            push(@ids_command, "--eq", "isolation_country,usa", "--eq", "h_type,1");
        }
        elsif ($type =~ /^(h1|h3|h5)$/) {
            my $h_type_value = scalar(substr($type, 1)); # Remove the 'h' prefix and make it scalar for h_type
            push(@ids_command, "--eq", "h_type,$h_type_value");
        }

        # Run the command and save the output in a file
        open my $out_fh, ">", $id_file or die "Cannot open $id_file for writing: $!";
        run3(join(' ', @ids_command), undef, $out_fh);
        close $out_fh;
        print STDOUT "Downloaded genomes ${type} ids.\n";

        $download_status = process_id_file($id_file, $type, $fasta_file);
    }
    return $download_status;
}

# Process the ID file and download FASTA sequences if needed
sub process_id_file {
    my ($id_file, $type, $fasta_file) = @_;

    my $ids_count = `wc -l < $id_file`;
    chomp $ids_count;

    if ($ids_count <= 1) {
        print STDOUT "No genome ids fetched for type $type.\n";
        return 0;
    }

    print STDOUT "Downloading genomes ${type} fasta.\n";
    open my $fh, "<", $id_file or die "Cannot open $id_file: $!";
    <$fh>; # Skip header
    open my $fasta_out_fh, ">", $fasta_file or die "Cannot open $fasta_file: $!";

    while (my $line = <$fh>) {
        chomp $line;
        my ($gid, $attr, $bioacc, $classification) = split "\t", $line;
        $genome{$gid}->{$type} = { $attributes{$type} => $attr, bioacc => $bioacc };

        # Only segment 4 for influenza types
        if (!$classification && (exists $influenza_types{$type} ? $attr == 4 : 1)) {
            my $fasta_command = "p3-genome-fasta --contig $gid";
            print STDOUT "Downloading fasta for ${type}: $fasta_command\n";
            run3($fasta_command, undef, $fasta_out_fh);
        }
    }

    close $fh;
    close $fasta_out_fh;

    print STDOUT "Download genomes ${type} fasta.\n";
    return 1;
}

# Submit downloaded FASTA files for classification
sub submit_data {
    my %job_ids;

    foreach my $type (keys %types) {
        my $fasta_file = "$download_dir/${type}.fasta";
        next unless -e $fasta_file;

        print STDOUT "Submitting genome ${type} for classification.\n";
        my $ws_folder = "$ws_path/${type}";
        my $ws_file = "$ws_folder/${date}_${type}.fasta";

        run3([ "p3-cp", "-f", $fasta_file, "ws:$ws_file" ]);

        # Prepare JSON file for classification
        my $json_data = {
            output_file      => $date,
            output_path      => $ws_folder,
            input_fasta_file => $ws_file,
            input_source     => "fasta_file",
            virus_type       => $types{$type}
        };
        my $json_file = "$download_dir/${type}.json";
        open my $json_fh, ">", $json_file or die "Cannot open $json_file for writing: $!";
        print $json_fh encode_json($json_data);
        close $json_fh;

        # Submit the classification job
        my @job_out;
        run3([ "appserv-start-app", "SubspeciesClassification", $json_file ], undef, \@job_out);
        my $job_id = (split ' ', $job_out[0])[2];
        $job_ids{$type} = $job_id;
        print STDOUT "Job submitted for ${type} with ID: $job_id\n";
    }

    return %job_ids;
}

# Fetch results after job completion
sub fetch_result {
    my (%job_ids) = @_;
    mkdir $result_dir unless -d $result_dir;

    while (%job_ids) {
        foreach my $type (keys %job_ids) {
            my $job_id = $job_ids{$type};

            my @status_output;
            run3([ "p3-job-status", $job_id ], undef, \@status_output);
            my $status = $status_output[0];
            print STDOUT "Job status for ${type}: $status\n";

            if ($status =~ /completed/) {
                my $result_file = "$result_dir/${type}_result.tsv";
                run3([ "p3-cp", "-r", "ws:$ws_path/${type}/.${date}/details/result.tsv", $result_file ]);
                delete $job_ids{$type};
                print STDOUT "Classification completed for ${type}.\n";
            }
            elsif ($status =~ /failed/) {
                delete $job_ids{$type};
                print STDERR "Job failed for ${type}.\n";
            }
        }

        sleep(3);
    }
}

# Process classification results
sub process_result {
    my %biosample;

    foreach my $type (keys %types) {
        # Keep a copy of the IDs file
        my $id_name = "${type}_ids.txt";
        my $id_file = "$download_dir/$id_name";
        my $destination_file = "$result_dir/$id_name";

        if (-e $id_file) {
            copy($id_file, $destination_file) or warn "Failed to copy $id_file: $!";
            print STDOUT "Copied $id_file to $result_dir.\n";
        }
        else {
            warn "ID file $id_file does not exist for ${type}.\n";
        }

        my $result_file = "$result_dir/${type}_result.tsv";
        next unless -e $result_file;

        open my $in_fh, "<", $result_file or die "Cannot open $result_file: $!";
        <$in_fh>; # Skip header

        while (my $line = <$in_fh>) {
            my ($ids, $classification) = split "\t", $line;

            # Reset classification if it starts with 'Sequence'
            $classification = "" if $classification =~ /^Sequence/;
            $classification =~ s/\s+//g; # Remove all whitespace

            # Example ids: 11320.665951.con.0001_segment_11320.665958.con.0001_segment
            for my $id (split '_', $ids) {
                next if $id eq "segment"; # Skip "segment"

                my ($gid) = $id =~ /^([\d]+\.[\d]+)\./; # Extract the part before the second dot
                my $bioacc = $genome{$gid}->{$type}->{bioacc};

                $biosample{$bioacc} = $classification;
                $genome{$gid}->{$type}->{classification} = $classification;
            }
        }
        close $in_fh;

        # Update using biosample_accession classification value if not found for gid
        my $temp_file = "$result_dir/${type}_result.tmp";
        open my $out_fh, ">", $temp_file or die "Unable to open $temp_file for write: $!";
        foreach my $gid (keys %genome) {
            my $attribute_name = $attributes{$type};
            my $attribute_value = $genome{$gid}->{$type}->{$attribute_name};
            my $classification = $genome{$gid}->{$type}->{classification} // "";
            chomp $classification;

            if ($classification) {
                print $out_fh "$gid\t$classification\t$attribute_name $attribute_value\n";
            }
            else {
                my $bioacc = $genome{$gid}->{$type}->{bioacc} // "";
                my $class_from_biosample = $bioacc ? ($biosample{$bioacc} // "") : "";
                chomp $class_from_biosample;

                if ($class_from_biosample) {
                    print $out_fh "$gid\t$class_from_biosample\t$attribute_name $attribute_value\n";
                    $genome{$gid}->{$type}->{classification} = $class_from_biosample;
                }
            }
        }
        close $out_fh;
    }

    generate_summary();
}

# Generate a final summary of the results
sub generate_summary {
    my $result_file = "$result_dir/subspecies_classification.tsv";
    open my $out_fh, ">", $result_file or die "Cannot open $result_file: $!";

    # Print header by extracting values from the clades map
    my @headers = ("genome_id", values %clades);
    print $out_fh join("\t", @headers) . "\n";

    my @json_output;

    foreach my $gid (keys %genome) {
        my @row = ($gid); # Start with genome_id
        my %entry = (genome_id => $gid);

        # Loop through each type in %clades and fetch classification
        foreach my $type (keys %clades) {
            my $classification = $genome{$gid}->{$type}->{classification} || '';
            push @row, $classification; # Add classification to the row

            my $field_name = $clades{$type};
            $entry{$field_name} = { set => $classification } if $classification ne '';
        }

        # Only write the row if at least one classification is present
        if (grep {$_ ne ''} @row[1 .. $#row]) {
            # Ignore genome_id when checking
            print STDOUT join("\t", @row) . "\n";
            print $out_fh join("\t", @row) . "\n";
            push @json_output, \%entry;
        }
    }
    close $out_fh;
    print STDOUT "Summary result generated at $result_file.\n";

    if (defined $update_file && $update_file ne '') {
        open my $json_fh, ">", $update_file or die "Cannot open $update_file: $!";
        print $json_fh to_json(\@json_output, { pretty => 1 });
        close $json_fh;
        print STDOUT "Update JSON written to $update_file\n";
    }
}
