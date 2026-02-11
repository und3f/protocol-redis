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
        'meta-spec'    => {version => 2},
        resources      => {
            license    => ['http://dev.perl.org/licenses/'],
            repository => {
                type => 'git',
                url  => 'https://github.com/und3f/protocol-redis.git',
                web  => 'https://github.com/und3f/protocol-redis',
            },
            bugtracker =>
              {web => 'https://github.com/und3f/protocol-redis/issues'},
        },
        prereqs => {
            develop => {
                requires => {
                    'Pod::Readme::Brief'   => 0,
                    'Pod::Simple::JustPod' => 0,
                }
            }
        },
    },

    MIN_PERL_VERSION => '5.008001',
    PREREQ_PM        => {
        'Carp'         => 0,
        'Math::BigInt' => 0,
        'Test::More'   => '0.94',
    },
    test => {TESTS => 't/*.t'},
);

sub MY::postamble {
    return <<'MAKEFILE';
create_distdir: README

README: scripts/create-readme.pl
	$(PERLRUN) $< $@

README.pod: scripts/create-readme-pod.pl
	$(PERLRUN) $< $@

format:
	find . -iname "*.p[ml]" -o -name "*.t" | xargs perltidy

check-format:
	find . -iname "*.p[ml]" -o -name "*.t" | xargs perltidy --assert-tidy
MAKEFILE
}
