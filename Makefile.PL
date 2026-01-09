use 5.008001;
 
use strict;
use warnings;
 
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'Protocol::Redis',
    VERSION_FROM  => 'lib/Protocol/Redis.pm',
    ABSTRACT_FROM => 'lib/Protocol/Redis.pm',
    AUTHOR        => 'Serhii Zasenko <sergii@zasenko.name>',
    LICENSE       => 'perl_5',

    META_MERGE => {
        dynamic_config => 0,
        'meta-spec'    => { version => 2 },
        resources      => {
            license    => [ 'http://dev.perl.org/licenses/' ],
            repository => {
                type   => 'git',
                url    => 'https://github.com/und3f/protocol-redis.git',
                web    => 'https://github.com/und3f/protocol-redis',
            },
            bugtracker => { web => 'https://github.com/und3f/protocol-redis/issues' },
        },
    },

    MIN_PERL_VERSION => '5.008001',
    PREREQ_PM => {
        'Carp' => 0,
        'Math::BigInt' => 0,
        'Test::More' => '0.94',
    },
    test => {TESTS =>  't/*.t'},

);
