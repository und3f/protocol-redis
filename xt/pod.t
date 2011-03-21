#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;

my $min_tp = 1.44;
eval "use Test::Pod $min_tp";
plan skip_all => "Test::Pod $min_tp required for this test!" if $@;

all_pod_files_ok();
