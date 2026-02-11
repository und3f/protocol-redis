#!/usr/bin/env perl

use Pod::Readme::Brief;

$PROTOCOL_REDIS_PM = "lib/Protocol/Redis.pm";

sub create_plaintext_readme($) {
    my $out_filename = $_[0];

    my $readme = Pod::Readme::Brief->new(
        do { open my $fh, '<', $PROTOCOL_REDIS_PM or die $!; readline $fh; }
    );

    open(my $fh, '>', $out_filename)
      or die "Could not open '$out_filename': $!";
    print $fh $readme->render(installer => 'eumm');
    close($fh);
}

if (scalar(@ARGV) < 1) {
    print STDERR "Usage: $0 README\n";
    exit(2);
}

my $readme_out = $ARGV[0];
print "Write plaintext readme '$readme_out'... ";
create_plaintext_readme($readme_out);
print "Done.\n";
