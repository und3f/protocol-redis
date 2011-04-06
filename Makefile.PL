#!/usr/bin/env perl

use 5.008001;

use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'Protocol::Redis',
    AUTHOR        => 'Sergey Zasenko <undef@cpan.org>',
    LICENSE       => 'perl',
    ABSTRACT_FROM => 'lib/Protocol/Redis.pm',
    VERSION_FROM  => 'lib/Protocol/Redis.pm',

    BUILD_REQUIRES => {'Test::More' => '0.88'},
    PREREQ_PM      => {
        'Carp'       => 0,
        'List::Util' => 0
    },
    META_MERGE => {
        resources => {repository => 'http://github.com/und3f/protocol-redis',}
    }
);
