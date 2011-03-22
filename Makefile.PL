#!/usr/bin/env perl

use 5.008001;

use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(
    NAME         => 'Protocol::Redis',
    VERSION_FROM => 'lib/Protocol/Redis.pm',
    AUTHOR       => 'Sergey Zasenko <undef@cpan.org>',
    PREREQ_PM    => {
        'Carp'       => 0,
        'List::Util' => 0
    },
    test => {TESTS => 't/*.t'}
);
