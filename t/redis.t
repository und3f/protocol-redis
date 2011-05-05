#!usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 1;
use Protocol::Redis::Test;

protocol_redis_ok 'Protocol::Redis', 1;
