#!usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 5;

use Protocol::Redis::Test;

use_ok 'Protocol::Redis';

my $redis = new_ok 'Protocol::Redis';

protocol_redis_ok $redis, 1;

# Test old stuff
$redis->parse("+foo\r\n");
is_deeply $redis->get_command, {type => '+', data => 'foo'},
  'get_command works';

my $r;
$redis->on_command(
    sub {
        my ($redis, $message) = @_;
        push @$r, $message;
    }
);
$redis->parse("+foo\r\n");
is_deeply $r, [{type => '+', data => 'foo'}], 'on_command works';

