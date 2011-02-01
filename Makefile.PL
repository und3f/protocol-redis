#!/usr/bin/env perl

use 5.008001;

use strict;
use warnings;

# Son, when you participate in sporting events,
# it's not whether you win or lose, it's how drunk you get.
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
