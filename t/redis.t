#!usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 3;
use Protocol::Redis::Test;

protocol_redis_ok 'Protocol::Redis', 1;
protocol_redis_ok 'Protocol::Redis', 2;
protocol_redis_ok 'Protocol::Redis', 3;
