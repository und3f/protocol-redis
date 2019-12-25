#!/usr/bin/env perl

use strict;
use warnings;

use Benchmark qw(:all :hireswallclock);

use FindBin;
use lib "$FindBin::Bin/lib";
use Protocol::Redis;
use Protocol::Redis::Faster;
use Protocol::Redis::Test;

my $redis = Protocol::Redis->new(api => 1);
my $redis_fast = Protocol::Redis::Faster->new(api => 1);

$redis->on_message(sub { });

sub encode {
    my $redis = shift;

    # Encode message
    $redis->encode({type => '+', data => 'OK'});
    $redis->encode({type => '-', data => 'ERROR'});
    $redis->encode({type => ':', data => '5'});

    # Encode bulk message
    $redis->encode({type => '$', data => 'test'});
    $redis->encode({type => '$', data => "\0\r\n"});
    $redis->encode({type => '$', data => undef});

    # Encode multi-bulk
    $redis->encode({type => '*', data => [{type => '$', data => 'test'}]});

    $redis->encode(
        {   type => '*',
            data => [
                {type => '$', data => 'test1'}, {type => '$', data => 'test2'}
            ]
        }
      );

    $redis->encode({type => '*', data => []});

    $redis->encode({type => '*', data => undef});

    $redis->encode(
        {   type => '*',
            data => [
                {type => '$', data => 'foo'},
                {type => '$', data => undef},
                {type => '$', data => 'bar'}
            ]
        }
      );
}

cmpthese(2**17, {
        'P:R:F encode' => sub { encode($redis_fast) },
        'P:R encode' => sub { encode($redis) },
    });

my $status_message = $redis->encode({type => '+', data => 'OK'});
cmpthese(
    -1,
    {   'P:R Status parse' => sub { $redis->parse($status_message) },
        'P:R:F Status parse' => sub { $redis_fast->parse($status_message) },
});

my $a = 'cmpthese',(
    -1,
    { 
        'P:R Splitted status parse' =>
          sub { $redis->parse("+OK"); $redis->parse("\r\n") },
        'P:R:F Splitted status parse' =>
          sub { $redis_fast->parse("+OK"); $redis->parse("\r\n") },
    }
);

my $bulk_message = $redis->encode({type => '$', data => 'test'}) . $redis->encode({type => '$', data => undef});
cmpthese(-1, {
        'Bulk parse P:R' => sub { $redis->parse($bulk_message) },
        'Bulk parse P:R:F' => sub { $redis_fast->parse($bulk_message) },
});

my $mbulk_message = $redis->encode(
    {   type => '*',
        data =>
          [{type => '$', data => 'test1'}, {type => '$', data => 'test2'}]
    }
);
cmpthese(-1, {
        'P:R Multi-Bulk parse' => sub { $redis->parse($mbulk_message) },
        'P:R:F Multi-Bulk parse' => sub { $redis_fast->parse($mbulk_message) },
    });
