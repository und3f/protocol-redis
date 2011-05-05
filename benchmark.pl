#!/usr/bin/env perl

use strict;
use warnings;

use Benchmark qw(:all :hireswallclock);

use FindBin;
use lib "$FindBin::Bin/lib";
use Protocol::Redis;

my $redis = Protocol::Redis->new(api => 1);

$redis->on_message(sub { });

my $status_message = $redis->encode({type => '+', data => 'OK'});
timethese(
    0,
    {   '1. Status parse' => sub { $redis->parse($status_message) },
        '2. Splitted status parse' =>
          sub { $redis->parse("+OK"); $redis->parse("\r\n") },
    }
);

my $bulk_message = $redis->encode({type => '$', data => 'test'});
timethese(0, {'1. Bulk parse' => sub { $redis->parse($bulk_message) },});

my $mbulk_message = $redis->encode(
    {   type => '*',
        data =>
          [{type => '$', data => 'test1'}, {type => '$', data => 'test2'}]
    }
);
timethese(0, {'Multi-Bulk parse' => sub { $redis->parse($mbulk_message) },});
