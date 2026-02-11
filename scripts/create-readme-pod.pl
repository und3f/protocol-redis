#!/usr/bin/env perl

use Pod::Select qw(podselect);

$PROTOCOL_REDIS_PM = "lib/Protocol/Redis.pm";

if (scalar(@ARGV) < 1) {
    print STDERR "Usage: $0 README.pod\n";
    exit(2);
}

my $readme_out = $ARGV[0];

print "Write $readme_out... ";
podselect({-output => $readme_out}, ($PROTOCOL_REDIS_PM));
print "Done.\n";

