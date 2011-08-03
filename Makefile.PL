use inc::Module::Install;

name 'Protocol-Redis';
all_from 'lib/Protocol/Redis.pm';

requires 'Carp';
requires 'List::Util';
requires 'Test::More' => '0.88';

tests 't/*.t';

resources
  bugtracker => 'https://github.com/und3f/protocol-redis/issues',
  repository => 'http://github.com/und3f/protocol-redis';

WriteAll;
