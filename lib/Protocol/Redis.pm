package Protocol::Redis;

use strict;
use warnings;
use 5.008_001;

our $VERSION = '1.0011';

require Carp;

sub new {
    my $class = shift;
    $class = ref $class if ref $class;

    my $self = {@_};

    return unless $self->{api} == '1';

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
my $rn = "\r\n";

sub encode {
    my $self = shift;

    my $encoded_message = '';
    while (@_) {
        my $message = shift;

        # Bulk string
        if ($message->{type} eq '$') {
            if (defined $message->{data}) {
                $encoded_message .= '$' . length($message->{data}) . "\r\n" . $message->{data} . "\r\n";
            }
            else {
                $encoded_message .= "\$-1\r\n";
            }
        }
        # Array (multi bulk)
        elsif ($message->{type} eq '*') {
            if (defined $message->{data}) {
                $encoded_message .= '*' . scalar(@{$message->{data}}) . "\r\n";
                unshift @_, @{$message->{data}};
            }
            else {
                $encoded_message .= "*-1\r\n";
            }
        }
        # String, error, integer
        elsif (exists $simple_types{$message->{type}}) {
            $encoded_message .= $message->{type} . $message->{data} . "\r\n";
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

        # Simple Strings, Errors, Integers
        if (exists $simple_types{$message->{type}}) {
            $message->{data} = delete $message->{_argument};
        }
        # Bulk Strings
        elsif ($message->{type} eq '$') {
            if ($message->{_argument} eq '-1') {
                $message->{data} = undef;
            }
            elsif (length($$buffer) >= $message->{_argument} + 2) {
                $message->{data} = substr $$buffer, 0, $message->{_argument}, '';
                substr $$buffer, 0, 2, ''; # Remove \r\n
            }
            else {
                return # Wait more data
            }
        }
        # Arrays
        elsif ($message->{type} eq '*') {
            if ($message->{_argument} eq '-1') {
                $message->{data} = undef;
            } else {
                $message->{data} = [];
                if ($message->{_argument} > 0) {
                    $message = $self->{_message} = {_parent => $message};
                    next;
                }
            }
        }
        # Invalid input
        else {
            Carp::croak(qq/Unexpected input "$self->{_message}{type}"/);
        }

        delete $message->{_argument};
        delete $self->{_message};

        # Fill parents with data
        while (my $parent = delete $message->{_parent}) {
            push @{$parent->{data}}, $message;

            if (@{$parent->{data}} < $parent->{_argument}) {
                $message = $self->{_message} = {_parent => $parent};
                next CHUNK;
            }
            else {
                $message = $parent;
                delete $parent->{_argument};
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
    my $redis = Protocol::Redis->new(api => 1) or die "API v1 not supported";

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

=head1 DESCRIPTION

Redis protocol parser/encoder with asynchronous capabilities and L<pipelining|http://redis.io/topics/pipelining> support.

=head1 APIv1

Protocol::Redis APIv1 uses
"L<Unified Request Protocol|http://redis.io/topics/protocol>" for message
encoding/parsing and supports methods described further. Client libraries
should specify API version during Protocol::Redis construction.

=head2 C<new>

    my $redis = Protocol::Redis->new(api => 1)
        or die "API v1 not supported";

Construct Protocol::Redis object with specific API version support.
If specified API version not supported constructor returns undef.
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

Sergey Zasenko, C<undef@cpan.org>.

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

Copyright (C) 2011-2019, Sergey Zasenko.

This program is free software, you can redistribute it and/or modify it under
the same terms as Perl 5.10.

=cut
