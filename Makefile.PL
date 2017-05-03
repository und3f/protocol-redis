use 5.010001;
 
use strict;
use warnings;
 
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'Protocol::Redis',
    VERSION_FROM  => 'lib/Protocol/Redis.pm',
    ABSTRACT_FROM => 'lib/Protocol/Redis.pm',
    AUTHOR        => 'Sergey Zasenko <sergey@zasenko.name>',
    LICENSE       => 'perl',

    META_MERGE => {
        resources => {
            license    => 'http://dev.perl.org/licenses/',
            repository => 'https://github.com/und3f/protocol-redis',
            bugtracker => 'https://github.com/und3f/protocol-redis/issues',
        },
    },

    PREREQ_PM => {
        'Carp' => 0,
        'List::Util' => 0,
    },
    TEST_REQUIRES => {
        'Test::More' => '0.88'
    },
    test => {TESTS =>  't/*.t'},

);
