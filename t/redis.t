#!usr/bin/env perl

use strict;
use warnings;
use utf8;

use Test::More tests => 34;

use_ok 'Protocol::Redis';

my $redis = new_ok 'Protocol::Redis';

# Simple test
$redis->parse("+test\r\n");

is_deeply $redis->get_message,
  {type => '+', data => 'test'},
  'simple message';

is_deeply $redis->get_message, undef, 'queue is empty';

$redis->parse(":1\r\n");

is_deeply $redis->get_message, {type => ':', data => '1'}, 'simple number';

# Unicode test
$redis->parse("+привет\r\n");

is_deeply $redis->get_message,
  {type => '+', data => 'привет'},
  'unicode string';

# Chunked message
$redis->parse('-tes');
$redis->parse("t2\r\n");
is_deeply $redis->get_message,
  {type => '-', data => 'test2'},
  'chunked string';

# Two messages together
$redis->parse("+test");
$redis->parse("1\r\n-test");
$redis->parse("2\r\n");
is_deeply
  [$redis->get_message, $redis->get_message],
  [{type => '+', data => 'test1'}, {type => '-', data => 'test2'}],
  'first stick message';

# Bulk message
$redis->parse("\$4\r\ntest\r\n");
is_deeply $redis->get_message,
  {type => '$', data => 'test'},
  'simple bulk message';

$redis->parse("\$5\r\ntes");
$redis->parse("t2\r\n");
is_deeply $redis->get_message,
  {type => '$', data => 'test2'},
  'splitted bulk message';

# Nil bulk message
$redis->parse("\$-1\r\n");

is_deeply $redis->get_message,
  {type => '$', data => undef},
  'nil bulk message';

# splitted bulk message
$redis->parse(join("\r\n", '$4', 'test', '+OK'));
$redis->parse("\r\n");
is_deeply $redis->get_message,
  {type => '$', data => 'test'}, 'splitted message';
is_deeply $redis->get_message, {type => '+', data => 'OK'};

# Multi bulk message!
$redis->parse("*1\r\n\$4\r\ntest\r\n");

is_deeply $redis->get_message,
  {type => '*', data => [{type => '$', data => 'test'}]},
  'simple multibulk message';

# Multi bulk message with multiple arguments
$redis->parse("*3\r\n\$5\r\ntest1\r\n");
$redis->parse("\$5\r\ntest2\r\n");
$redis->parse("\$5\r\ntest3\r\n");

is_deeply $redis->get_message,
  { type => '*',
    data => [
        {type => '$', data => 'test1'},
        {type => '$', data => 'test2'},
        {type => '$', data => 'test3'}
    ]
  },
  'multi argument multi-bulk message';

$redis->parse("*0\r\n");
is_deeply $redis->get_message,
  {type => '*', data => []},
  'multi-bulk nil result';

# Does it work?
$redis->parse("\$4\r\ntest\r\n");
is_deeply $redis->get_message,
  {type => '$', data => 'test'},
  'everything still works';

# Multi bulk message with status items
$redis->parse(join("\r\n", '*2', '+OK', '$4', 'test', ''));
is_deeply $redis->get_message,
  { type => '*',
    data => [{type => '+', data => 'OK'}, {type => '$', data => 'test'}]
  };

# splitted multi-bulk
$redis->parse(join("\r\n", '*1', '$4', 'test', '+OK'));
$redis->parse("\r\n");

is_deeply $redis->get_message,
  {type => '*', data => [{type => '$', data => 'test'}]};
is_deeply $redis->get_message, {type => '+', data => 'OK'};


# Parsing with cb
my $r = [];
$redis->on_message(
    sub {
        my ($redis, $message) = @_;

        push @$r, $message;
    }
);

$redis->parse("+foo\r\n");
$redis->parse("\$3\r\nbar\r\n");

is_deeply $r,
  [{type => '+', data => 'foo'}, {type => '$', data => 'bar'}],
  'parsing with callback';

$redis->on_message(undef);

# Test old stuff
$redis->parse("+foo\r\n");
is_deeply $redis->get_command, {type => '+', data => 'foo'},
  'get_command works';

$r = undef;
$redis->on_command(
    sub {
        my ($redis, $message) = @_;
        push @$r, $message;
    }
);
$redis->parse("+foo\r\n");
is_deeply $r, [{type => '+', data => 'foo'}], 'on_command works';

# Encode message
is $redis->encode('+', 'OK'),    "+OK\r\n",    'encode status';
is $redis->encode('-', 'ERROR'), "-ERROR\r\n", 'encode error';
is $redis->encode(':', '5'),     ":5\r\n",     'encode integer';

# Encode bulk message
is $redis->encode('$', 'test'), "\$4\r\ntest\r\n", 'encode bulk';
is $redis->encode('$', undef),  "\$-1\r\n",        'encode nil bulk';

# Encode multi-bulk
is $redis->encode('*', ['test']), "\*1\r\n\$4\r\ntest\r\n",
  'encode multi-bulk';
is $redis->encode('*', [qw/test1 test2/]),
  "\*2\r\n\$5\r\ntest1\r\n\$5\r\ntest2\r\n",
  'encode multi-bulk';

is $redis->encode('*', []), "\*0\r\n", 'encode empty multi-bulk';

is $redis->encode('*', undef), "\*-1\r\n", 'encode nil multi-bulk';

is $redis->encode('*', ['foo', undef, 'bar']),
  "\*3\r\n\$3\r\nfoo\r\n\$-1\r\n\$3\r\nbar\r\n",
  'encode multi-bulk with nil element';

# Encode data from hash
is $redis->encode({type => '+', data => 'OK'}), "+OK\r\n",
  'encode status from hash';
