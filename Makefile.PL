use 5.008001;
 
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

    MIN_PERL_VERSION => '5.008001',
    PREREQ_PM => {
        'Carp' => 0,
        'Test::More' => '0.94',
    },
    TEST_REQUIRES => {
        'Test::More' => '0.88'
    },
    test => {TESTS =>  't/*.t'},

);
