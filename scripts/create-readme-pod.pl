#!/usr/bin/env perl

use Pod::Simple::JustPod;

$PROTOCOL_REDIS_PM = "lib/Protocol/Redis.pm";

sub create_pod_readme($) {
    my $readme_out = $_[0];

    my $parser = Pod::Simple::JustPod->new();

    open my $fh, ">$readme_out" or die "Can't write to $readme_out $!";
    $parser->output_fh($fh);

    $parser->parse_file($PROTOCOL_REDIS_PM);

    close $fh or die "Can't close $outfile: $!";
}

if (scalar(@ARGV) < 1) {
    print STDERR "Usage: $0 README.pod\n";
    exit(2);
}

print "Write $readme_out... ";
create_pod_readme $ARGV[0];
print "Done.\n";

