#!usr/bin/env perl

use strict;
use warnings;
use utf8;

use Test::More tests => 16;

use_ok 'Protocol::Redis';

my $redis = new_ok 'Protocol::Redis';

# Simple test
$redis->parse("+test\r\n");

is_deeply $redis->get_command,
  {type => '+', data => 'test'},
  'simple command';

is_deeply $redis->get_command,
  undef,
  'queue is empty';

$redis->parse(":1\r\n");

is_deeply $redis->get_command,
  {type => ':', data => '1'},
  'simple number';

# Unicode test
$redis->parse("+привет\r\n");

is_deeply $redis->get_command,
  {type => '+', data => 'привет'},
  'unicode string';

# Chunked command
$redis->parse('-tes');
$redis->parse("t2\r\n");
is_deeply $redis->get_command,
  {type => '-', data => 'test2'},
  'chunked string';

# Two commands together
$redis->parse("+test");
$redis->parse("1\r\n-test");
$redis->parse("2\r\n");
is_deeply
  [$redis->get_command, $redis->get_command],
  [{type => '+', data => 'test1'}, {type => '-', data => 'test2'}],
  'first stick command';

# Bulk command
$redis->parse("\$4\r\ntest\r\n");
is_deeply $redis->get_command,
  {type => '$', data => 'test'},
  'simple bulk command';

$redis->parse("\$5\r\ntes");
$redis->parse("t2\r\n");
is_deeply $redis->get_command,
  {type => '$', data => 'test2'},
  'splitted bulk command';

# Nil bulk command
$redis->parse("\$-1\r\n");

is_deeply $redis->get_command,
  {type => '$', data => undef},
  'nil bulk command';

# Multi bulk command!
$redis->parse("*1\r\n\$4\r\ntest\r\n");

is_deeply $redis->get_command,
  {type => '*', data => ['test']},
  'simple multibulk command';

# Multi bulk command with multiple arguments
$redis->parse("*3\r\n\$5\r\ntest1\r\n");
$redis->parse("\$5\r\ntest2\r\n");
$redis->parse("\$5\r\ntest3\r\n");

is_deeply $redis->get_command,
  {type => '*', data => [qw/test1 test2 test3/]},
  'multi argument multi-bulk command';

$redis->parse("*0\r\n");
is_deeply $redis->get_command,
  {type => '*', data => []},
  'multi-bulk nil result';

# Does it work?
$redis->parse("\$4\r\ntest\r\n");
is_deeply $redis->get_command,
  {type => '$', data => 'test'},
  'everything still works';

# Parsing with cb
my $r = [];
$redis->on_command(
    sub {
        my ($redis, $command) = @_;

        push @$r, $command;
    }
);

$redis->parse("+foo\r\n");
$redis->parse("\$3\r\nbar\r\n");

is_deeply $r,
  [{type => '+', data => 'foo'}, {type => '$', data => 'bar'}],
  'parsing with callback';

