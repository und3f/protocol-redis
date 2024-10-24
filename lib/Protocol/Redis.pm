package Protocol::Redis;

use strict;
use warnings;
use 5.008_001;

our $VERSION = '1.0021';

require Carp;

sub new {
    my $class = shift;
    $class = ref $class if ref $class;

    my $self = {@_};

    Carp::croak(qq/Unknown Protocol::Redis API version $self->{api}/)
      unless $self->{api} == 1 or $self->{api} == 2 or $self->{api} == 3;

    bless $self, $class;

    $self->on_message(delete $self->{on_message});
    $self->{_messages} = [];

    $self;
}

sub api {
    my $self = shift;

    $self->{api};
}

my %simple_types = ('+' => 1, '-' => 1, ':' => 1);
my %blob_types = ('$' => 1, '!' => 1, '=' => 1);
my %aggregate_types = ('*' => 1, '%' => 1, '~' => 1, '|' => 1, '>' => 1);
my $rn = "\r\n";

sub encode {
    my $self = shift;

    my $encoded_message = '';
    while (@_) {
        my $message = shift;

        # Attributes
        if (defined $message->{attributes}) {
            my %append_message = %$message;
            my $attributes = delete $append_message{attributes};
            $encoded_message .= '|' . keys(%$attributes) . "\r\n";
            unshift @_, (map { ({type => '$', data => $_}, $attributes->{$_}) }
                sort keys %$attributes), \%append_message;
        }

        # Bulk string, Blob error, Verbatim string
        elsif ($message->{type} eq '$' or ($self->{api} == 3 and exists $blob_types{$message->{type}})) {
            if (defined $message->{data}) {
                $encoded_message .= $message->{type} . length($message->{data}) . "\r\n" . $message->{data} . "\r\n";
            }
            else {
                $encoded_message .= "$message->{type}-1\r\n";
            }
        }
        # Array, Set, Push
        elsif ($message->{type} eq '*'
               or ($self->{api} == 3 and ($message->{type} eq '~' or $message->{type} eq '>'))) {
            if (defined $message->{data}) {
                $encoded_message .= $message->{type} . scalar(@{$message->{data}}) . "\r\n";
                unshift @_, @{$message->{data}};
            }
            else {
                $encoded_message .= "$message->{type}-1\r\n";
            }
        }
        # Map
        elsif ($self->{api} == 3 and $message->{type} eq '%') {
            if (ref $message->{data} eq 'ARRAY') {
                $encoded_message .= $message->{type} . int(@{$message->{data}} / 2) . "\r\n";
                unshift @_, @{$message->{data}};
            } else {
                $encoded_message .= $message->{type} . keys(%{$message->{data}}) . "\r\n";
                unshift @_, map { ({type => '$', data => $_}, $message->{data}{$_}) }
                    sort keys %{$message->{data}};
            }
        }
        # String, error, integer, big number
        elsif (exists $simple_types{$message->{type}}
               or ($self->{api} == 3 and $message->{type} eq '(')) {
            $encoded_message .= $message->{type} . $message->{data} . "\r\n";
        }
        # Double
        elsif ($self->{api} == 3 and $message->{type} eq ',') {
            # inf
            if ($message->{data} == $message->{data} * 2) {
                $encoded_message .= ',' . ($message->{data} > 0 ? '' : '-') . "inf\r\n";
            }
            # nan
            elsif ($message->{data} != $message->{data}) {
                $encoded_message .= ",nan\r\n";
            }
            else {
                $encoded_message .= $message->{type} . $message->{data} . "\r\n";
            }
        }
        # Null
        elsif ($self->{api} == 3 and $message->{type} eq '_') {
            $encoded_message .= "_\r\n";
        }
        # Boolean
        elsif ($self->{api} == 3 and $message->{type} eq '#') {
            $encoded_message .= '#' . ($message->{data} ? 't' : 'f') . "\r\n";
        }
        else {
            Carp::croak(qq/Unknown message type $message->{type}/);
        }
    }

    return $encoded_message;
}

sub get_message {
    shift @{$_[0]->{_messages}};
}

sub on_message {
    my ($self, $cb) = @_;
    $self->{_on_message_cb} = $cb || \&_gather_messages;
}

sub parse {
    my $self        = shift;
    $self->{_buffer}.= shift;

    my $message = $self->{_message} ||= {};
    my $buffer  = \$self->{_buffer};

    CHUNK:
    while ((my $pos = index($$buffer, "\r\n")) != -1) {
        # Check our state: are we parsing new message or completing existing
        if (!$message->{type}) {
            if ($pos < 1) {
                Carp::croak(qq/Unexpected input "$$buffer"/);
            }

            $message->{type} = substr $$buffer, 0, 1;
            $message->{_argument}  = substr $$buffer, 1, $pos - 1;
            substr $$buffer, 0, $pos + 2, ''; # Remove type + argument + \r\n
        }

        # Streamed String Parts - must be checked for first
        if ($self->{api} == 3 and $message->{_streaming}) {
            unless ($message->{type} eq ';') {
                Carp::croak(qq/Unexpected input "$message->{type}"/);
            }

            if ($message->{_argument} == 0) {
                $message = delete $message->{_streaming};
            }
            elsif (length($$buffer) >= $message->{_argument} + 2) {
                my $streaming = delete $message->{_streaming};
                $streaming->{data} .= substr $$buffer, 0, $message->{_argument}, '';
                substr $$buffer, 0, 2, ''; # Remove \r\n
                $message = $self->{_message} = {_streaming => $streaming};
                next;
            }
            else {
                return # Wait more data
            }
        }
        # Simple Strings, Errors, Integers
        elsif (exists $simple_types{$message->{type}}) {
            $message->{data} = delete $message->{_argument};
        }
        # Null
        elsif ($self->{api} == 3 and $message->{type} eq '_') {
            delete $message->{_argument};
            $message->{data} = undef;
        }
        # Booleans
        elsif ($self->{api} == 3 and $message->{type} eq '#') {
            $message->{data} = !!(delete($message->{_argument}) eq 't');
        }
        # Doubles
        elsif ($self->{api} == 3 and $message->{type} eq ',') {
            $message->{data} = delete $message->{_argument};
            $message->{data} = 'nan' if $message->{data} =~ m/^[-+]?nan/i;
        }
        # Big Numbers
        elsif ($self->{api} == 3 and $message->{type} eq '(') {
            require Math::BigInt;
            $message->{data} = Math::BigInt->new(delete $message->{_argument});
        }
        # Bulk/Blob Strings, Blob Errors, Verbatim Strings
        elsif ($message->{type} eq '$' or ($self->{api} == 3 and exists $blob_types{$message->{type}})) {
            if ($message->{_argument} eq '-1') {
                $message->{data} = undef;
            }
            elsif ($self->{api} == 3 and $message->{type} eq '$' and $message->{_argument} eq '?') {
                $message->{data} = '';
                $message = $self->{_message} = {_streaming => $message};
                next;
            }
            elsif (length($$buffer) >= $message->{_argument} + 2) {
                $message->{data} = substr $$buffer, 0, $message->{_argument}, '';
                substr $$buffer, 0, 2, ''; # Remove \r\n
            }
            else {
                return # Wait more data
            }
        }
        # Arrays, Maps, Sets, Attributes, Push
        elsif ($message->{type} eq '*' or ($self->{api} == 3 and exists $aggregate_types{$message->{type}})) {
            if ($message->{_argument} eq '-1') {
                $message->{data} = undef;
            } else {
                if ($self->{api} == 3 and ($message->{type} eq '%' or $message->{type} eq '|')) {
                    $message->{data} = {};
                } else {
                    $message->{data} = [];
                }

                if (($self->{api} == 3 and $message->{_argument} eq '?') or $message->{_argument} > 0) {
                    $message = $self->{_message} = {_parent => $message};
                    next;
                }
            }
            # Populate empty attributes for next message if we reach here
            if ($self->{api} == 3 and $message->{type} eq '|') {
                $message->{attributes} = {};
                delete $message->{type};
                delete $message->{data};
                delete $message->{_argument};
                next;
            }
        }
        # Streamed Aggregate End
        elsif ($self->{api} == 3 and $message->{type} eq '.'
               and $message->{_parent} and $message->{_parent}{_argument} eq '?') {
            $message = delete $message->{_parent};
            delete $message->{_elements};
        }
        # Invalid input
        else {
            Carp::croak(qq/Unexpected input "$self->{_message}{type}"/);
        }

        delete $message->{_argument};
        delete $self->{_message};

        # Fill parents with data
        while (my $parent = delete $message->{_parent}) {
            # Map key or value
            if ($self->{api} == 3 and ($parent->{type} eq '%' or $parent->{type} eq '|')) {
                if (exists $parent->{_key}) {
                    $parent->{_elements}++;
                    $parent->{data}{delete $parent->{_key}} = $message;
                } else {
                    $parent->{_key} = $message->{data};
                }
            }
            # Array or set element
            else {
                $parent->{_elements}++;
                push @{$parent->{data}}, $message;
            }

            # Do we need more elements?
            if (($self->{api} == 3 and $parent->{_argument} eq '?')
                or ($parent->{_elements} || 0) < $parent->{_argument}) {
                $message = $self->{_message} = {_parent => $parent};
                next CHUNK;
            }

            # Aggregate is complete
            $message = $parent;
            delete $message->{_argument};
            delete $message->{_elements};
            delete $message->{_key};

            # Attributes apply to the following message
            if ($self->{api} == 3 and $message->{type} eq '|') {
                $self->{_message} = $message;
                $message->{attributes} = delete $message->{data};
                delete $message->{type};
                next CHUNK;
            }
        }

        $self->{_on_message_cb}->($self, $message);
        $message = $self->{_message} = {};
    }
}

sub _gather_messages {
    push @{$_[0]->{_messages}}, $_[1];
}

1;
__END__

=head1 NAME

Protocol::Redis - Redis protocol parser/encoder with asynchronous capabilities.

=head1 SYNOPSIS

    use Protocol::Redis;
    my $redis = Protocol::Redis->new(api => 1);

    $redis->parse("+foo\r\n");

    # get parsed message
    my $message = $redis->get_message;
    print "parsed message: ", $message->{data}, "\n";

    # asynchronous parsing interface
    $redis->on_message(sub {
        my ($redis, $message) = @_;
        print "parsed message: ", $message->{data}, "\n";
    });

    # parse pipelined message
    $redis->parse("+bar\r\n-error\r\n");

    # create message
    print "Get key message:\n",
      $redis->encode({type => '*', data => [
         {type => '$', data => 'string'},
         {type => '+', data => 'OK'}
    ]});

    # RESP3 supported with api 3 specified
    my $redis = Protocol::Redis->new(api => 3);

    print $redis->encode({type => '%', data => {
        null   => {type => '_', data => undef},
        booleans => {type => '~', data => [
            {type => '#', data => 1},
            {type => '#', data => 0},
        ]},
    }, attributes => {
        coordinates => {type => '*', data => [
            {type => ',', data => '36.001516'},
            {type => ',', data => '-78.943319'},
        ]},
    });

    $redis->parse("~3\r\n+x\r\n+y\r\n+z");
    # sets represented in the protocol the same as arrays
    my %set = map {($_->{data} => 1)} @{$redis->get_message};
    print join ',', keys %set; # x,y,z in unspecified order

=head1 DESCRIPTION

Redis protocol parser/encoder with asynchronous capabilities and L<pipelining|http://redis.io/topics/pipelining> support.

=head1 APIv1

Protocol::Redis APIv1 uses
"L<Unified Request Protocol|http://redis.io/topics/protocol>" for message
encoding/parsing and supports methods described further. Client libraries
should specify API version during Protocol::Redis construction.

API version 1 corresponds to the protocol now known as
L<RESP2|https://github.com/redis/redis-specifications/blob/master/protocol/RESP2.md>
and can also thusly be specified as API version 2.

=head1 APIv3

API version 3 supports the same methods as API version 1, corresponding to the
L<RESP3|https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md>
protocol. RESP3 contains support for several additional data types (null,
boolean, double, big number, blob error, verbatim string, map, and set), data
attributes, streamed strings and aggregate data, and explicitly specified push
data so that asynchronous and synchronous responses can share a connection. A
client must request RESP3 support from the server with the HELLO command to use
these features.

=head2 C<new>

    my $redis = Protocol::Redis->new(api => 1);

Construct Protocol::Redis object with specific API version support.
If specified API version not supported constructor should raise an exception.
Client libraries should always specify API version.

=head2 C<parse>

    $redis->parse("*2\r\n$4ping\r\n\r\n");

Parse Redis protocol chunk.

=head2 C<get_message>

    while (my $message = $redis->get_message) {
        ...
    }

Get parsed message or undef.

=head2 C<on_message>

    $redis->on_message(sub {
        my ($redis, $message) = @_;

    }

Calls callback on each parsed message.

=head2 C<encode>
    
    my $string = $redis->encode({type => '+', data => 'test'});
    $string = $redis->encode(
        {type => '*', data => [
            {type => '$', data => 'test'}]});

Encode data into redis message.

=head2 C<api>

    my $api_version = $redis->api;

Get API version.


=head1 SUPPORT

=head2 IRC

    #redis on irc.perl.org
    
=head1 DEVELOPMENT

=head2 Repository

    http://github.com/und3f/protocol-redis

=head1 AUTHOR

Serhii Zasenko, C<undef@cpan.org>.

=head1 CREDITS

In alphabetical order

=over 2

Dan Book (Grinnz)

David Leadbeater (dgl)

Jan Henning Thorsen (jhthorsen)

Viacheslav Tykhanovskyi (vti)

Yaroslav Korshak (yko)

=back

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011-2024, Serhii Zasenko.

This program is free software, you can redistribute it and/or modify it under
the same terms as Perl 5.10.

=cut
