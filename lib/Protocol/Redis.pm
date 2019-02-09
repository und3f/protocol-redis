package Protocol::Redis;

use strict;
use warnings;
use 5.008_001;

our $VERSION = 1.0006;

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

my %message_type_encoders = (
    '+' => \&_encode_string,
    '-' => \&_encode_string,
    ':' => \&_encode_string,
    '$' => \&_encode_bulk,
    '*' => \&_encode_multi_bulk,
);

sub encode {
    my ($self, $message) = @_;

    if (my $encoder = $message_type_encoders{$message->{type}}) {
        $encoder->($self, $message);
    }
    else {
        Carp::croak(qq/Unknown message type $message->{type}/);
    }
}

sub _encode_string {
    my ($self, $message) = @_;

    $message->{type} . $message->{data} . "\r\n";
}

sub _encode_bulk {
    my ($self, $message) = @_;

    my $data = $message->{data};

    return '$-1' . "\r\n"
      unless defined $data;

    '$' . length($data) . "\r\n" . $data . "\r\n";
}

sub _encode_multi_bulk {
    my ($self, $message) = @_;

    my $data = $message->{data};

    return '*-1' . "\r\n"
      unless defined $data;

    my $e_message = '*' . scalar(@$data) . "\r\n";
    foreach my $element (@$data) {
        $e_message .= $self->encode($element);
    }

    $e_message;
}


sub get_message {
    shift @{$_[0]->{_messages}};
}

sub on_message {
    my ($self, $cb) = @_;
    $self->{_on_message_cb} = $cb || \&_gather_messages;
}

sub parse {
    my $self = shift;
    $self->{buf} .= shift;

    my $curr = $self->{curr} ||= {};
    my $buf  = \$self->{buf};

    CHUNK:
    while (length $$buf) {

        # Look for message type and get the actual data,
        # length of the bulk string or the size of the array
        if (!$curr->{type}) {
            my $pos = index $$buf, "\r\n";
            return if $pos < 0; # Wait for more data

            $curr->{type} = substr $$buf, 0, 1;
            $curr->{len}  = substr $$buf, 1, $pos - 1;
            substr $$buf, 0, $pos + 2, ''; # Remove type + length/data + \r\n
        }

        # Bulk Strings
        if ($curr->{type} eq '$') {
            if ($curr->{len} == -1) {
                $curr->{data} = undef;
            }
            elsif (length($$buf) - 2 < $curr->{len}) {
                return; # Wait for more data
            }
            else {
                $curr->{data} = substr $$buf, 0, $curr->{len}, '';
            }

            substr $$buf, 0, 2, ''; # Remove \r\n
        }

        # Simple Strings, Errors, Integers
        elsif ($curr->{type} eq '+' or $curr->{type} eq '-' or $curr->{type} eq ':') {
            $curr->{data} = delete $curr->{len};
        }

        # Arrays
        elsif ($curr->{type} eq '*') {
            $curr->{data} = $curr->{len} < 0 ? undef : [];

            # Fill the array with data
            if ($curr->{len} > 0) {
                $curr = $self->{curr} = {parent => $curr};
                next CHUNK;
            }
        }

        # Should not come to this
        else {
            Carp::croak(qq/Unknown message type $curr->{type}/);
        }

        # Fill parent array with data
        while (my $parent = delete $curr->{parent}) {
            delete $curr->{len};
            push @{$parent->{data}}, $curr;

            if (@{$parent->{data}} < $parent->{len}) {
                $curr = $self->{curr} = {parent => $parent};
                next CHUNK;
            }
            else {
                $curr = $self->{curr} = $parent;
            }
        }

        # Emit a complete message
        delete $curr->{len};
        $self->{_on_message_cb}->($self, $curr);
        $curr = $self->{curr} = {};
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

David Leadbeater (dgl)

Viacheslav Tykhanovskyi (vti)

Yaroslav Korshak (yko)

=back

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011-2017, Sergey Zasenko.

This program is free software, you can redistribute it and/or modify it under
the same terms as Perl 5.10.

=cut
