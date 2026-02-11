package Protocol::Redis::Test;

use strict;
use warnings;

require Exporter;

our @ISA    = qw(Exporter);
our @EXPORT = qw(protocol_redis_ok);

use Test::More;
require Carp;

sub protocol_redis_ok {
    my ($redis_class, $api_version) = @_;

    subtest 'Protocol::Redis tests' => sub {
        plan tests => 3;

        use_ok $redis_class;

        _test_unknown_version($redis_class);

        if ($api_version == 1 or $api_version == 2) {
            _apiv1_ok($redis_class);
        }
        elsif ($api_version == 3) {
            _apiv3_ok($redis_class);
        }
        else {
            Carp::croak(qq/Unknown Protocol::Redis API version $api_version/);
        }
    };
}

sub _apiv1_ok {
    my $redis_class = shift;

    subtest 'Protocol::Redis APIv1 ok' => sub {
        plan tests => 45;

        _test_version_1($redis_class);
    }
}

sub _apiv3_ok {
    my $redis_class = shift;

    subtest 'Protocol::Redis APIv3 ok' => sub {
        plan tests => 151;

        _test_version_3($redis_class);
    }
}

sub _test_version_1 {
    my $redis_class = shift;

    my $redis = new_ok $redis_class, [api => 1];

    can_ok $redis, 'parse', 'api', 'on_message', 'encode';

    is $redis->api, 1, '$redis->api';

    # Parsing method tests
    $redis->on_message(undef);
    _parse_string_ok($redis);
    _parse_blob_ok($redis);
    _parse_array_ok($redis);
    _parse_null_bulk_ok($redis);

    # on_message works
    _on_message_ok($redis);

    # Encoding method tests
    _encode_ok($redis);
    _encode_v1_ok($redis);
}

sub _test_version_3 {
    my $redis_class = shift;

    my $redis = new_ok $redis_class, [api => 3];

    can_ok $redis, 'parse', 'api', 'on_message', 'encode';

    is $redis->api, 3, '$redis->api';

    # Parsing method tests
    $redis->on_message(undef);

    # simple string, error string, number
    _parse_string_ok($redis);
    _parse_null_ok($redis);
    _parse_boolean_ok($redis);
    _parse_double_ok($redis);
    _parse_big_number_ok($redis);
    _parse_blob_ok($redis, '$');     # blob string
    _parse_blob_ok($redis, '!');     # blob error
    _parse_verbatim_ok($redis);      # verbatim string
    _parse_array_ok($redis, '*');    # array
    _parse_array_ok($redis, '~');    # set
    _parse_array_ok($redis, '>');    # push
    _parse_map_ok($redis);
    _parse_attribute_ok($redis);
    _parse_streamed_string_ok($redis);
    _parse_streamed_aggregate_ok($redis);

    # on_message works
    _on_message_ok($redis);

    # Encoding method tests
    _encode_ok($redis);
    _encode_v3_ok($redis);
}

sub _parse_string_ok {
    my $redis = shift;

    # Simple test
    $redis->parse("+test\r\n");

    is_deeply $redis->get_message,
      {type => '+', data => 'test'},
      'simple message';

    is_deeply $redis->get_message, undef, 'queue is empty';

    $redis->parse(":1\r\n");

    is_deeply $redis->get_message, {type => ':', data => '1'},
      'simple number';

    # Chunked message
    $redis->parse('-tes');
    $redis->parse("t2\r\n");
    is_deeply $redis->get_message,
      {type => '-', data => 'test2'},
      'chunked string';

    # Two chunked messages together
    $redis->parse("+test");
    $redis->parse("1\r\n-test");
    $redis->parse("2\r\n");
    is_deeply
      [$redis->get_message, $redis->get_message],
      [{type => '+', data => 'test1'}, {type => '-', data => 'test2'}],
      'first stick message';

    # Pipelined
    $redis->parse("+OK\r\n-ERROR\r\n");
    is_deeply
      [$redis->get_message, $redis->get_message],
      [{type => '+', data => 'OK'}, {type => '-', data => 'ERROR'}],
      'pipelined status messages';
}

sub _parse_blob_ok {
    my $redis = shift;
    my $type  = shift || '$';

    # Bulk message
    $redis->parse("${type}4\r\ntest\r\n");
    is_deeply $redis->get_message,
      {type => $type, data => 'test'},
      "simple $type blob message";

    $redis->parse("${type}5\r\ntes");
    $redis->parse("t2\r\n");
    is_deeply $redis->get_message,
      {type => $type, data => 'test2'},
      "chunked $type blob message";

    # Two chunked bulk messages
    $redis->parse(join("\r\n", "${type}4", 'test', '+OK'));
    $redis->parse("\r\n");
    is_deeply $redis->get_message,
      {type => $type, data => 'test'}, "two chunked $type blob messages";
    is_deeply $redis->get_message, {type => '+', data => 'OK'};

    # Pipelined bulk message
    $redis->parse(join("\r\n", ("${type}3", 'ok1'), ("${type}3", 'ok2'), ''));
    is_deeply [$redis->get_message, $redis->get_message],
      [{type => $type, data => 'ok1'}, {type => $type, data => 'ok2'}],
      "pipelined $type blob message";

    # Binary test
    $redis->parse(join("\r\n", "${type}4", pack('C4', 0, 1, 2, 3), ''));

    is_deeply [unpack('C4', $redis->get_message->{data})],
      [0, 1, 2, 3],
      'binary data';

    # Blob message with newlines
    $redis->parse("${type}8\r\none\r\ntwo\r\n");

    is_deeply $redis->get_message,
      {type => $type, data => "one\r\ntwo"},
      "$type blob message with newlines";

    # Empty blob message
    $redis->parse("${type}0\r\n\r\n");

    is_deeply $redis->get_message,
      {type => $type, data => ''},
      "$type empty blob message";
}

sub _parse_verbatim_ok {
    my $redis = shift;

    # Verbatim message
    $redis->parse("=8\r\ntxt:test\r\n");
    is_deeply $redis->get_message,
      {type => '=', data => 'test', format => 'txt'},
      "simple verbatim string message";

    $redis->parse("=9\r\ntxt:tes");
    $redis->parse("t2\r\n");
    is_deeply $redis->get_message,
      {type => '=', data => 'test2', format => 'txt'},
      "chunked verbatim string message";

    # Two chunked verbatim messages
    $redis->parse(join("\r\n", '=8', 'txt:test', '=6', 'mkd:OK'));
    $redis->parse("\r\n");
    is_deeply $redis->get_message,
      {type => '=', data => 'test', format => 'txt'},
      "two chunked verbatim string messages";
    is_deeply $redis->get_message,
      {type => '=', data => 'OK', format => 'mkd'};

    # Pipelined verbatim message
    $redis->parse(join("\r\n", ('=7', 'txt:ok1'), ('=7', 'txt:ok2'), ''));
    is_deeply [$redis->get_message, $redis->get_message], [
        {type => '=', data => 'ok1', format => 'txt'},
        {type => '=', data => 'ok2', format => 'txt'}
      ],
      "pipelined verbatim string message";

    # Binary test
    $redis->parse(join("\r\n", '=8', 'txt:' . pack('C4', 0, 1, 2, 3), ''));

    is_deeply [unpack('C4', $redis->get_message->{data})],
      [0, 1, 2, 3],
      'binary data';

    # Blob message with newlines
    $redis->parse("=12\r\ntxt:one\r\ntwo\r\n");

    is_deeply $redis->get_message,
      {type => '=', data => "one\r\ntwo", format => 'txt'},
      "verbatim string message with newlines";

    # Empty blob message
    $redis->parse("=4\r\ntxt:\r\n");

    is_deeply $redis->get_message,
      {type => '=', data => '', format => 'txt'},
      "empty verbatim string message";
}

sub _parse_array_ok {
    my $redis = shift;
    my $type  = shift || '*';

    # Array message
    $redis->parse("${type}1\r\n\$4\r\ntest\r\n");

    is_deeply $redis->get_message,
      {type => $type, data => [{type => '$', data => 'test'}]},
      "simple $type array message";

    # Array message with multiple arguments
    $redis->parse("${type}3\r\n\$5\r\ntest1\r\n");
    $redis->parse("\$5\r\ntest2\r\n");
    $redis->parse("\$5\r\ntest3\r\n");

    is_deeply $redis->get_message, {
        type => $type,
        data => [
            {type => '$', data => 'test1'},
            {type => '$', data => 'test2'},
            {type => '$', data => 'test3'}
        ]
      },
      "multi argument $type array message";

    # Nested array
    $redis->parse("${type}2\r\n${type}2\r\n+test1\r\n+test2\r\n+test3\r\n");

    is_deeply $redis->get_message, {
        type => $type,
        data => [{
                type => $type,
                data => [
                    {type => '+', data => 'test1'},
                    {type => '+', data => 'test2'},
                ]
            },
            {type => '+', data => 'test3'},
        ]
      },
      "nested $type array message";

    $redis->parse("${type}0\r\n");
    is_deeply $redis->get_message,
      {type => $type, data => []},
      "$type array empty result";

    # Does it work?
    $redis->parse("\$4\r\ntest\r\n");
    is_deeply $redis->get_message,
      {type => '$', data => 'test'},
      'everything still works';

    # Array message with status items
    $redis->parse(join("\r\n", ("${type}2", '+OK', '$4', 'test'), ''));
    is_deeply $redis->get_message, {
        type => $type,
        data => [{type => '+', data => 'OK'}, {type => '$', data => 'test'}]
      };

    # splitted array
    $redis->parse(join("\r\n", ("${type}1", '$4', 'test'), '+OK'));
    $redis->parse("\r\n");

    is_deeply $redis->get_message,
      {type => $type, data => [{type => '$', data => 'test'}]};
    is_deeply $redis->get_message, {type => '+', data => 'OK'};

    # Another splitted array message
    $redis->parse("${type}4\r\n\$0\r\n\r\n\$0\r\n");
    $redis->parse("\r\n\$5\r\ntest2\r\n");
    $redis->parse("\$5\r\ntest3\r");
    $redis->parse("\n");
    is_deeply $redis->get_message, {
        type => $type,
        data => [
            {type => '$', data => ''},
            {type => '$', data => ''},
            {type => '$', data => 'test2'},
            {type => '$', data => 'test3'}
        ]
      };

    # Complex string
    $redis->parse("${type}4\r\n");
    $redis->parse("\$5\r\ntest1\r\n\$0\r\n\r\n:42\r\n+test3\r\n\$5\r\n123");
    $redis->parse("45\r\n");
    is_deeply $redis->get_message, {
        type => $type,
        data => [
            {type => '$', data => 'test1'},
            {type => '$', data => ''},
            {type => ':', data => 42},
            {type => '+', data => 'test3'}
        ]
      };
    is_deeply $redis->get_message, {
        type => '$',
        data => '12345',
      };

    # pipelined array
    $redis->parse(
        join("\r\n",
            ("${type}2", '$3', 'ok1', '$3', 'ok2'),
            ("${type}1", '$3', 'ok3'),
            '')
    );

    is_deeply $redis->get_message, {
        type => $type,
        data => [{type => '$', data => 'ok1'}, {type => '$', data => 'ok2'}]
      };
    is_deeply $redis->get_message,
      {type => $type, data => [{type => '$', data => 'ok3'}]};
}

sub _parse_null_bulk_ok {
    my $redis = shift;

    # Nil bulk message
    $redis->parse("\$-1\r\n");

    my $message = $redis->get_message;
    ok defined($message) && !defined($message->{data}), 'nil bulk message';

    $redis->parse("*-1\r\n");
    $message = $redis->get_message;
    ok defined($message) && !defined($message->{data}),
      'multi-bulk nil result';
}

sub _parse_null_ok {
    my $redis = shift;

    # Null message
    $redis->parse("_\r\n");

    is_deeply $redis->get_message,
      {type => '_', data => undef},
      'null message';
}

sub _parse_boolean_ok {
    my $redis = shift;

    # Boolean true message
    $redis->parse("#t\r\n");

    is_deeply $redis->get_message,
      {type => '#', data => !!1},
      'boolean true message';

    # Boolean false message
    $redis->parse("#f\r\n");

    is_deeply $redis->get_message,
      {type => '#', data => !!0},
      'boolean false message';
}

sub _parse_double_ok {
    my $redis = shift;

    # Double message
    $redis->parse(",1.23\r\n");

    is_deeply $redis->get_message,
      {type => ',', data => '1.23'},
      'double message';

    # Integer double message
    $redis->parse(",42\r\n");

    is_deeply $redis->get_message,
      {type => ',', data => '42'},
      'integer double message';

    # Exponent double messages
    foreach my $double (qw(1e3 1.23e4 -4.2E-20 0.01e+7)) {
        $redis->parse(",$double\r\n");
        is_deeply $redis->get_message,
          {type => ',', data => $double},
          "received $double as double with exponent";
    }

    # Infinity message
    $redis->parse(",inf\r\n");
    my $message = $redis->get_message;
    is $message->{type}, ',', 'double message type';
    cmp_ok $message->{data}, '==', $message->{data} + 1, 'received infinity';
    cmp_ok $message->{data}, '>',  0, 'received positive infinity';

    # Negative infinity message
    $redis->parse(",-inf\r\n");
    $message = $redis->get_message;
    is $message->{type}, ',', 'double message type';
    cmp_ok $message->{data}, '==', $message->{data} - 1, 'received infinity';
    cmp_ok $message->{data}, '<',  0, 'received negative infinity';

    # NaN message
    $redis->parse(",nan\r\n");
    $message = $redis->get_message;
    is $message->{type}, ',', 'double message type';
    cmp_ok $message->{data}, '!=', $message->{data}, 'received NaN';

    # libc NaN compatibility
    foreach my $nan (qw(-nan NaN NAN nan(chars))) {
        $redis->parse(",$nan\r\n");
        my $message = $redis->get_message;
        is $message->{type}, ',', 'double message type';
        cmp_ok $message->{data}, '!=', $message->{data},
          "interpreted $nan as NaN double";
    }
}

sub _parse_big_number_ok {
    my $redis = shift;

    # Big number message
    $redis->parse("(3492890328409238509324850943850943825024385\r\n");
    my $message = $redis->get_message;
    is $message->{type}, '(', 'big number message type';
    is $message->{data}, '3492890328409238509324850943850943825024385',
      'big number data';

    # Negative big number message
    $redis->parse("(-3492890328409238509324850943850943825024385\r\n");
    $message = $redis->get_message;
    is $message->{type}, '(', 'big number message type';
    is $message->{data}, '-3492890328409238509324850943850943825024385',
      'negative big number data';

    # Simple big number message
    $redis->parse("(0\r\n");

    is_deeply $redis->get_message,
      {type => '(', data => '0'},
      'simple big number';
}

sub _parse_map_ok {
    my $redis = shift;

    # Map message
    $redis->parse("%1\r\n+one\r\n+two\r\n");

    is_deeply $redis->get_message,
      {type => '%', data => {one => {type => '+', data => 'two'}}},
      'simple map message';

    # Empty map message
    $redis->parse("%0\r\n");

    is_deeply $redis->get_message,
      {type => '%', data => {}},
      'empty map message';

    # Complex map message
    $redis->parse("%3\r\n\$5\r\ntest1\r\n");
    $redis->parse(":1\r\n+test2\r\n,0.01\r\n:3\r\n_\r\n");

    is_deeply $redis->get_message, {
        type => '%',
        data => {
            test1 => {type => ':', data => '1'},
            test2 => {type => ',', data => '0.01'},
            '3'   => {type => '_', data => undef},
        }
      },
      'complex map message';
}

sub _parse_set_ok {
    my $redis = shift;

    # Set message
    $redis->parse("~5\r\n+test1\r\n\$5\r\ntest2\r\n:42\r\n#t\r\n_\r\n");

    is_deeply $redis->get_message, {
        type => '~',
        data => [
            {type => '+', data => 'test1'},
            {type => '$', data => 'test2'},
            {type => ':', data => '42'},
            {type => '#', data => !!1},
            {type => '_', data => undef},
        ]
      },
      'set message';

    # Empty set message
    $redis->parse("~0\r\n");

    is_deeply $redis->get_message,
      {type => '~', data => []},
      'empty set message';

    # Set message with duplicates
    $redis->parse("~3\r\n+test1\r\n+test2\r\n+test1\r\n");

    is_deeply $redis->get_message, {
        type => '~',
        data => [
            {type => '+', data => 'test1'},
            {type => '+', data => 'test2'},
            {type => '+', data => 'test1'},
        ]
      },
      'set message with duplicates';

    # Set message with non-duplicate but equal strings
    $redis->parse("~8\r\n+test1\r\n\$5\r\ntest1\r\n");
    $redis->parse(":1\r\n,1\r\n#t\r\n");
    $redis->parse("_\r\n+\r\n\$0\r\n\r\n");

    is_deeply $redis->get_message, {
        type => '~',
        data => [
            {type => '+', data => 'test1'},
            {type => '$', data => 'test1'},
            {type => ':', data => '1'},
            {type => ',', data => '1'},
            {type => '#', data => !!1},
            {type => '_', data => undef},
            {type => '+', data => ''},
            {type => '$', data => ''},
        ]
      },
      'set message with non-duplicate but equal strings';
}

sub _parse_attribute_ok {
    my $redis = shift;

    # Message with attributes
    $redis->parse("|1\r\n+total\r\n:5\r\n*2\r\n");
    $redis->parse("\$5\r\ntest1\r\n\$5\r\ntest2\r\n");

    is_deeply $redis->get_message, {
        type => '*',
        data =>
          [{type => '$', data => 'test1'}, {type => '$', data => 'test2'},],
        attributes => {
            total => {type => ':', data => '5'},
        }
      },
      'message with attributes';

    # Multiple attributes
    $redis->parse("|3\r\n+x\r\n,0.5\r\n+y\r\n,-3.4\r\n+z\r\n:42\r\n+OK\r\n");

    is_deeply $redis->get_message, {
        type       => '+',
        data       => 'OK',
        attributes => {
            x => {type => ',', data => '0.5'},
            y => {type => ',', data => '-3.4'},
            z => {type => ':', data => '42'},
        }
      },
      'message with multiple attributes';

    # Empty attributes
    $redis->parse("|0\r\n-ERR no response\r\n");

    is_deeply $redis->get_message,
      {type => '-', data => 'ERR no response', attributes => {}},
      'message with empty attributes';

    # Embedded attributes
    $redis->parse("*2\r\n|2\r\n+min\r\n:0\r\n+max\r\n:10\r\n:5\r\n");
    $redis->parse("|2\r\n+min\r\n:4\r\n+max\r\n:8\r\n:7\r\n");

    is_deeply $redis->get_message, {
        type => '*',
        data => [{
                type       => ':',
                data       => '5',
                attributes => {
                    min => {type => ':', data => '0'},
                    max => {type => ':', data => '10'},
                }
            }, {
                type       => ':',
                data       => '7',
                attributes => {
                    min => {type => ':', data => '4'},
                    max => {type => ':', data => '8'},
                }
            },
        ]
      },
      'message with embedded attributes';

    # Aggregate attributes
    $redis->parse("|1\r\n+array\r\n*2\r\n+test1\r\n+test2\r\n+test\r\n");

    is_deeply $redis->get_message, {
        type       => '+',
        data       => 'test',
        attributes => {
            array => {
                type => '*',
                data => [
                    {type => '+', data => 'test1'},
                    {type => '+', data => 'test2'},
                ]
            }
        }
      },
      'message with aggregate attribute values';
}

sub _parse_push_ok {
    my $redis = shift;

    # Simple push message
    $redis->parse(">2\r\n\$5\r\ntest1\r\n:42\r\n");

    is_deeply $redis->get_message, {
        type => '>',
        data => [{type => '$', data => 'test1'}, {type => ':', data => '42'},]
      },
      'simple push message';

    # Empty push message
    $redis->parse(">0\r\n");

    is_deeply $redis->get_message,
      {type => '>', data => []},
      'empty push message';
}

sub _parse_streamed_string_ok {
    my $redis = shift;

    # Simple streamed string
    $redis->parse("\$?\r\n;4\r\ntest\r\n;2\r\nxy\r\n;0\r\n");

    is_deeply $redis->get_message,
      {type => '$', data => 'testxy'},
      'simple streamed string';

    # Empty streamed string
    $redis->parse("\$?\r\n;0\r\n");

    is_deeply $redis->get_message,
      {type => '$', data => ''},
      'empty streamed string';

    # Charwise streamed string
    $redis->parse("\$?\r\n");
    $redis->parse(";1\r\n$_\r\n") for 'a' .. 'z';
    $redis->parse(";0\r\n");

    is_deeply $redis->get_message,
      {type => '$', data => join('', 'a' .. 'z')},
      'string streamed by character';
}

sub _parse_streamed_aggregate_ok {
    my $redis = shift;
    my $type  = shift || '*';

    # Simple streamed aggregate
    $redis->parse("${type}?\r\n+test1\r\n+test2\r\n.\r\n");

    if ($type eq '%') {
        is_deeply $redis->get_message, {
            type => $type,
            data => {
                test1 => {type => '+', data => 'test2'},
            }
          },
          "simple $type streamed aggregate message";
    }
    else {
        is_deeply $redis->get_message, {
            type => $type,
            data => [
                {type => '+', data => 'test1'},
                {type => '+', data => 'test2'},
            ]
          },
          "simple $type streamed aggregate message";
    }

    # Empty streamed aggregate
    $redis->parse("${type}?\r\n.\r\n");

    is_deeply $redis->get_message,
      {type => $type, data => ($type eq '%' ? {} : [])},
      "empty $type streamed aggregate message";

    # Complex streamed aggregate
    $redis->parse("${type}?\r\n");
    $redis->parse("\$?\r\n;4\r\ntest\r\n;1\r\n1\r\n;0\r\n");
    $redis->parse("\$5\r\ntest$_\r\n") for 2 .. 9;
    $redis->parse("*?\r\n:10\r\n,11\r\n.\r\n");
    $redis->parse(".\r\n");

    if ($type eq '%') {
        is_deeply $redis->get_message, {
            type => $type,
            data => {
                test1 => {type => '$', data => 'test2'},
                test3 => {type => '$', data => 'test4'},
                test5 => {type => '$', data => 'test6'},
                test7 => {type => '$', data => 'test8'},
                test9 => {
                    type => '*',
                    data => [
                        {type => ':', data => '10'},
                        {type => ',', data => '11'},
                    ]
                },
            }
          },
          "complex $type streamed aggregate message";
    }
    else {
        is_deeply $redis->get_message, {
            type => $type,
            data => [
                {type => '$', data => 'test1'},
                (map { +{type => '$', data => "test$_"} } 2 .. 9), {
                    type => '*',
                    data => [
                        {type => ':', data => '10'},
                        {type => ',', data => '11'},
                    ]
                },
            ]
          },
          "complex $type streamed aggregate message";
    }
}

sub _on_message_ok {
    my $redis = shift;

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

    $r = [];
    $redis->parse(join("\r\n", ('+foo'), ('$3', 'bar'), ''));

    is_deeply $r,
      [{type => '+', data => 'foo'}, {type => '$', data => 'bar'}],
      'pipelined parsing with callback';

    $redis->on_message(undef);
}

sub _encode_ok {
    my $redis = shift;

    # Encode message
    is $redis->encode({type => '+', data => 'OK'}), "+OK\r\n",
      'encode status';
    is $redis->encode({type => '-', data => 'ERROR'}), "-ERROR\r\n",
      'encode error';
    is $redis->encode({type => ':', data => '5'}), ":5\r\n", 'encode integer';

    # Encode bulk message
    is $redis->encode({type => '$', data => 'test'}), "\$4\r\ntest\r\n",
      'encode bulk';
    is $redis->encode({type => '$', data => "\0\r\n"}), "\$3\r\n\0\r\n\r\n",
      'encode binary bulk';

    # Encode multi-bulk
    is $redis->encode({type => '*', data => [{type => '$', data => 'test'}]}),
      join("\r\n", ('*1', '$4', 'test'), ''),
      'encode multi-bulk';

    is $redis->encode({
            type => '*',
            data => [
                {type => '$', data => 'test1'}, {type => '$', data => 'test2'}
            ]
        }
      ),
      join("\r\n", ('*2', '$5', 'test1', '$5', 'test2'), ''),
      'encode multi-bulk';

    is $redis->encode({type => '*', data => []}), "\*0\r\n",
      'encode empty multi-bulk';
}

sub _encode_v1_ok {
    my $redis = shift;

    is $redis->encode({type => '$', data => undef}), "\$-1\r\n",
      'encode nil bulk';

    is $redis->encode({type => '*', data => undef}), "\*-1\r\n",
      'encode nil multi-bulk';

    is $redis->encode({
            type => '*',
            data => [
                {type => '$', data => 'foo'},
                {type => '$', data => undef},
                {type => '$', data => 'bar'}
            ]
        }
      ),
      join("\r\n", ('*3', '$3', 'foo', '$-1', '$3', 'bar'), ''),
      'encode multi-bulk with nil element';
}

sub _encode_v3_ok {
    my $redis = shift;

    # Encode simple RESP3 types
    is $redis->encode({type => '_', data => undef}), "_\r\n", 'encode null';

    is $redis->encode({type => '#', data => 1}), "#t\r\n",
      'encode boolean true';
    is $redis->encode({type => '#', data => 0}), "#f\r\n",
      'encode boolean false';

    is $redis->encode({type => ',', data => '1.3'}), ",1.3\r\n",
      'encode double';
    is $redis->encode({type => ',', data => '-1.2e-5'}), ",-1.2e-5\r\n",
      'encode negative double with exponent';
    is $redis->encode({type => ',', data => '10'}), ",10\r\n",
      'encode integer as double';
    is $redis->encode({type => ',', data => '0'}), ",0\r\n",
      'encode zero as double';
    is $redis->encode({type => ',', data => 9**9**9}), ",inf\r\n",
      'encode inf';
    is $redis->encode({type => ',', data => -9**9**9}), ",-inf\r\n",
      'encode negative inf';
    is $redis->encode({type => ',', data => -sin 9**9**9}), ",nan\r\n",
      'encode nan';

    is $redis->encode({
            type => '(',
            data => '3492890328409238509324850943850943825024385'
        }
      ),
      "(3492890328409238509324850943850943825024385\r\n",
      'encode big number';
    require Math::BigInt;
    is $redis->encode({
            type => '(',
            data => Math::BigInt->new(
                '-3492890328409238509324850943850943825024385')
        }
      ),
      "(-3492890328409238509324850943850943825024385\r\n",
      'encode bigint as big number';
    is $redis->encode({type => '(', data => '0'}), "(0\r\n",
      'encode zero as big number';

    # Encode blob RESP3 types
    is $redis->encode({type => '!', data => 'SYNTAX'}),
      "!6\r\nSYNTAX\r\n", 'encode blob error';
    is $redis->encode({type => '!', data => "\0\r\n"}),
      "!3\r\n\0\r\n\r\n", 'encode binary blob error';

    is $redis->encode({type => '=', data => '"foo"'}),
      "=9\r\ntxt:\"foo\"\r\n", 'encode verbatim string';
    is $redis->encode({type => '=', data => '"bar"', format => 'mkd'}),
      "=9\r\nmkd:\"bar\"\r\n", 'encode verbatim string with custom format';
    is $redis->encode({type => '=', data => "\0\r\n"}),
      "=7\r\ntxt:\0\r\n\r\n", 'encode binary verbatim string';

    # Encode aggregate RESP3 types
    is $redis->encode(
        {type => '%', data => {foo => {type => '+', data => 'bar'}}}),
      join("\r\n", '%1', '$3', 'foo', '+bar', ''),
      'encode map';
    is $redis->encode({
            type => '~',
            data => [{type => ':', data => 5}, {type => '+', data => 'test'}]
        }
      ),
      join("\r\n", '~2', ':5', '+test', ''),
      'encode set';
    is $redis->encode({
            type => '>',
            data =>
              [{type => '+', data => 'test'}, {type => ',', data => '4.2'}]
        }
      ),
      join("\r\n", '>2', '+test', ',4.2', ''),
      'encode push';

    # Encode attributes
    is $redis->encode({
            type       => '+',
            data       => 'test',
            attributes => {foo => {type => '+', data => 'bar'}}
        }
      ),
      join("\r\n", '|1', '$3', 'foo', '+bar', '+test', ''),
      'encode simple string with attributes';
    is $redis->encode({
            type       => '*',
            data       => [{type => '_', data => undef}],
            attributes => {test => {type => '#', data => 1}}
        }
      ),
      join("\r\n", '|1', '$4', 'test', '#t', '*1', '_', ''),
      'encode array with attributes';
    is $redis->encode({
            type => '~',
            data => [{
                    type       => ',',
                    data       => '-5.5',
                    attributes => {precision => {type => ':', data => 1}}
                }
            ]
        }
      ),
      join("\r\n", '~1', '|1', '$9', 'precision', ':1', ',-5.5', ''),
      'encode set with embedded attributes';
    is $redis->encode({
            type       => '+',
            data       => 'test',
            attributes => {
                array => {
                    type => '*',
                    data =>
                      [{type => ':', data => 1}, {type => ':', data => 2}]
                }
            }
        }
      ),
      join("\r\n", '|1', '$5', 'array', '*2', ':1', ':2', '+test', ''),
      'encode array attributes';
}

sub _test_unknown_version {
    my $redis_class = shift;

    eval { $redis_class->new(api => 0); };
    ok($@, 'unknown version raises an exception');
}

1;
__END__

=head1 NAME

Protocol::Redis::Test - reusable tests for Protocol::Redis implementations.

=head1 SYNOPSIS

    use Test::More tests => 1;
    use Protocol::Redis::Test;

    # Test Protocol::Redis API 
    protocol_redis_ok 'Protocol::Redis', 1;

=head1 DESCRIPTION

Reusable tests for Protocol::Redis implementations.

=head1 FUNCTIONS

=head2 C<protocol_redis_ok>

    protocol_redis_ok $redis_class, 1;

Check if $redis_class implementation of Protocol::Redis meets API version 1

=head1 SEE ALSO

L<Protocol::Redis>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010-2024, Serhii Zasenko

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=cut
