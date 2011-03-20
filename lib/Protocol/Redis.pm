package Protocol::Redis;

use strict;
use warnings;

use List::Util ();
require Carp;

our $VERSION = 0.9001;

sub new {
    my $class = shift;
    $class = ref $class if ref $class;

    my $self = {@_};
    bless $self, $class;

    $self->on_message($self->{on_message});
    $self->{_messages} = [];
    $self->_change_state(\&_state_new_message);

    $self;
}

sub encode {
    my ($self, $type, $data) = @_;

    if (ref $type eq 'HASH') {
        $data = $type->{data};
        $type = $type->{type};
    }

    if (List::Util::first { $type eq $_ } qw/+ - :/) {
        $self->_encode_string($type, $data);
    }
    elsif ($type eq '$') {
        $self->_encode_bulk($type, $data);
    }
    elsif ($type eq '*') {
        $self->_encode_multi_bulk($type, $data);
    }
    else {
        Carp::croak(qq/Unknown message type $type/);
    }
}

sub _encode_string {
    my ($self, $type, $data) = @_;

    $type . $data . "\r\n";
}

sub _encode_bulk {
    my ($self, $type, $data) = @_;

    return '$-1' . "\r\n"
      unless defined $data;

    '$' . length($data) . "\r\n" . $data . "\r\n";
}

sub _encode_multi_bulk {
    my ($self, $type, $data) = @_;

    return '*-1' . "\r\n"
      unless defined $data;

    my $message = '*' . scalar(@$data) . "\r\n";
    foreach my $element (@$data) {
        $message .= $self->_encode_bulk('$', $element);
    }
    $message;
}


sub get_message {
    my ($self) = @_;
    shift @{$self->{_messages}};
}

sub on_message {
    my ($self, $cb) = @_;
    $self->{_on_message_cb} = $cb;
}

sub get_command {
    my $self = shift;
    warn <<EOF;
Protocol::Redis->get_command renamed to Protocol::Redis->get_message
EOF
    $self->get_message(@_);
}

sub on_command {
    my $self = shift;
    warn <<EOF;
Protocol::Redis->on_command renamed to Protocol::Redis->on_message
EOF
    $self->on_message(@_);
}

sub parse {
    my ($self, $chunk) = @_;

    # Just pass chunk to current vertex
    while ($chunk = $self->{_state}->($self, $chunk)) { }
}

sub _change_state {
    my ($self, $new_state, $chunk) = @_;

    $self->{_state} = $new_state;

    # Pass rest of chunk to new vertex
    return $new_state->($self, $chunk) if $chunk;
    undef;
}

sub _message_parsed {
    my ($self, $chunk) = @_;

    my $message = delete $self->{_cmd};

    if (my $cb = $self->{_on_message_cb}) {
        $cb->($self, $message);
    }
    else {
        push @{$self->{_messages}}, $message;
    }

    #$self->_change_state(\&_state_new_message, $chunk);
    $self->{_state} = \&_state_new_message;

    $chunk;
}

sub _state_parse_message_type {
    my ($self, $chunk) = @_;

    my $cmd = substr $chunk, 0, 1, '';
    if (List::Util::first { $cmd eq $_ } (qw/+ - :/)) {
        $self->{_cmd}{type} = $cmd;
        return $self->_change_state(\&_state_string_message, $chunk);
    }
    elsif ($cmd eq '$') {
        $self->{_cmd}{type} = $cmd;
        return $self->_change_state(\&_state_bulk_message, $cmd . $chunk);
    }
    elsif ($cmd eq '*') {
        $self->{_cmd}{type} = $cmd;

        # it can
        return $self->_change_state(\&_state_multibulk_message, $chunk);

    }
    else {
        Carp::croak(qq/Unexpected input "$cmd"/);
    }

    undef;
}

sub _state_new_message {
    my ($self, $chunk) = @_;

    $self->{_cmd} = {type => undef, data => undef};

    $self->{_state_cb} = \&_message_parsed;

    $self->_change_state(\&_state_parse_message_type, $chunk);
}

sub _state_string_message {
    my ($self, $chunk) = @_;

    my $str = $self->{_state_string} .= $chunk;
    my $i = index $str, "\r\n";

    # string isn't full
    return undef if $i < 0;

    # We got full string
    my $result = substr $str, 0, $i, '';

    # Delete newline
    substr $str, 0, 2, '';

    delete $self->{_state_string};

    $self->{_cmd}{data} = $result;
    $self->{_state_cb}->($self, $str);
}

sub _state_bulk_message {
    my ($self, $chunk) = @_;

    my $bulk_state_cb = $self->{_state_cb};

    # Read bulk message size
    $self->{_state_cb} = sub {
        my ($self, $chunk) = @_;

        $self->{_bulk_size} = delete $self->{_cmd}{data};

        # Delete starting '$'
        unless (my $s = substr($self->{_bulk_size}, 0, 1, "") eq '$') {
            Carp::croak(qq/Unexpected bulk message start symbol "$s"/);
        }

        if ($self->{_bulk_size} == -1) {

            # Nil
            $self->{_cmd}{data} = undef;
            $bulk_state_cb->($self, $chunk);
        }
        else {
            $self->{_state_cb} = $bulk_state_cb;
            $self->_change_state(\&_state_bulk_message_data, $chunk);
        }
    };
    $self->_change_state(\&_state_string_message, $chunk);
}

sub _state_bulk_message_data {
    my ($self, $chunk) = @_;

    my $str = $self->{_state_string} .= $chunk;

    # String + newline parsed
    if (length $str >= $self->{_bulk_size} + 2) {
        my $result = substr $str, 0, $self->{_bulk_size}, '';

        # Delete ending newline
        substr $str, 0, 2, '';

        delete $self->{_state_string};
        delete $self->{_bulk_size};

        $self->{_cmd}{data} = $result;
        $self->{_state_cb}->($self, $str);
    }
}

sub _state_multibulk_message {
    my ($self, $chunk) = @_;

    my $mbulk_state_cb = delete $self->{_state_cb};
    my $data           = [];
    my $mbulk_process;

    my $arguments_num;

    $mbulk_process = sub {
        my ($self, $chunk) = @_;

        push @$data,
          { type => delete $self->{_cmd}{type},
            data => delete $self->{_cmd}{data}
          };

        if (scalar @$data == $arguments_num) {

            # Cleanup
            $mbulk_process = undef;
            delete $self->{_state_cb};

            # Return message
            $self->{_cmd}{type} = '*';
            $self->{_cmd}{data} = $data;
            $mbulk_state_cb->($self, $chunk);
        }
        else {

            # read next string
            $self->{_state_cb} = $mbulk_process;
            $self->_change_state(\&_state_parse_message_type, $chunk);
        }
    };

    $self->{_state_cb} = sub {
        my ($self, $chunk) = @_;

        # Number of Multi-Bulk message
        $arguments_num = delete $self->{_cmd}{data};
        if ($arguments_num < 1) {
            $mbulk_process = undef;
            $self->{_cmd}{data} = [];
            $mbulk_state_cb->($self, $chunk);
        }
        else {

            # We got messages
            $self->{_state_cb} = $mbulk_process;
            $self->_change_state(\&_state_parse_message_type, $chunk);
        }
    };

    # Get number of messages
    $self->_change_state(\&_state_string_message, $chunk);
}

1;
__END__

=head1 NAME

Protocol::Redis - Redis protocol parser/encoder

=head1 DESCRIPTION

Redis protocol parser/encoder.

=head1 METHODS

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
    
    my $string = $redis->encode('+', 'OK');
    $string = $redis->encode('$', 'result');
    $string = $redis->encode('*', ['foo', 'bar']);
    $string = $redis->encode({type => '+', data => 'test'});

Encode data into redis message

=head1 SUPPORT

=head2 IRC

    #ru.pm on irc.perl.org
    
=head1 DEVELOPMENT

=head2 Repository

    http://github.com/und3f/protocol-redis

=head1 AUTHOR

Sergey Zasenko, C<undef@cpan.org>.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011, Sergey Zasenko.

This program is free software, you can redistribute it and/or modify it under
the same terms as Perl 5.10.

=cut
