package Protocol::Redis;

use strict;
use warnings;

require Carp;

our $VERSION = 0.9001;

sub new {
    my $class = shift;
    $class = ref $class if ref $class;

    my $self = {@_};
    bless $self, $class;

    $self->on_message($self->{on_message});
    $self->{_messages} = [];

    $self->{_state} = \&_state_new_message;

    $self;
}

sub use_api {
    my ($self, $api_version) = @_;

    return $api_version == 1;
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
    1 while $chunk = $self->{_state}->($self, $chunk);
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

    $self->{_state} = \&_state_new_message;
    $chunk;
}

my %message_type_parsers = (
    '+' => \&_state_string_message,
    '-' => \&_state_string_message,
    ':' => \&_state_string_message,
    '$' => \&_state_bulk_message,
    '*' => \&_state_multibulk_message,
);

sub _state_parse_message_type {
    my ($self, $chunk) = @_;

    my $cmd = substr $chunk, 0, 1, '';

    if ($cmd) {
        if (my $parser = $message_type_parsers{$cmd}) {
            $self->{_cmd}{type} = $cmd;
            $self->{_state} = $parser;
            return $chunk;
        }
        else {
            Carp::croak(qq/Unexpected input "$cmd"/);
        }
    }
}

sub _state_new_message {
    my ($self, $chunk) = @_;

    $self->{_cmd} = {type => undef, data => undef};

    $self->{_state_cb} = \&_message_parsed;

    $self->{_state} = \&_state_parse_message_type;
    $chunk;
}

sub _state_string_message {
    my ($self, $chunk) = @_;

    my $str = $self->{_state_string} .= $chunk;
    my $i = index $str, "\r\n";

    # string isn't full
    return if $i < 0;

    # We got full string
    $self->{_cmd}{data} = substr $str, 0, $i, '';

    # Delete newline
    substr $str, 0, 2, '';

    delete $self->{_state_string};

    $self->{_state_cb}->($self, $str);
}

sub _state_bulk_message {
    my ($self, $chunk) = @_;

    my $bulk_state_cb = $self->{_state_cb};

    # Read bulk message size
    $self->{_state_cb} = sub {
        my ($self, $chunk) = @_;

        $self->{_bulk_size} = delete $self->{_cmd}{data};

        if ($self->{_bulk_size} == -1) {

            # Nil
            $self->{_cmd}{data} = undef;
            $bulk_state_cb->($self, $chunk);
        }
        else {
            $self->{_state_cb} = $bulk_state_cb;
            $self->{_state}    = \&_state_bulk_message_data;
            $chunk;
        }
    };
    $self->{_state} = \&_state_string_message;
    $chunk;
}

sub _state_bulk_message_data {
    my ($self, $chunk) = @_;

    my $str = $self->{_state_string} .= $chunk;

    # String + newline parsed
    return unless length $str >= $self->{_bulk_size} + 2;

    $self->{_cmd}{data} = substr $str, 0, $self->{_bulk_size}, '';

    # Delete ending newline
    substr $str, 0, 2, '';

    delete $self->{_state_string};
    delete $self->{_bulk_size};

    $self->{_state_cb}->($self, $str);
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
            $self->{_state}    = \&_state_parse_message_type;
            $chunk;
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
            $self->{_state}    = \&_state_parse_message_type;
            $chunk;
        }
    };

    # Get number of messages
    $self->{_state} = \&_state_string_message;
    $chunk;
}

1;
__END__

=head1 NAME

Protocol::Redis - Redis protocol parser/encoder with asynchronous capabilities.

=head1 SYNOPSIS

    use Protocol::Redis;
    my $redis = Protocol::Redis->new;

    # Init used API version
    $redis->use_api(1) or die "API v1 not supported";

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

=head1 APIv1 (DRAFT)

Protocol::Redis APIv1 uses
"L<Unified Request Protocol|http://redis.io/topics/protocol>" for message
encoding/parsing and supports methods described further. Client libraries
should call $redis->use_api(1) to start using APIv1.

=head2 C<use_api>

    $redis->use_api(1) or die "API v1 not supported";

Tell Protocol::Redis to use specific API version. Return false if API version
not supported. Client libraries should call this method first and check
returned value.

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

=head1 SUPPORT

=head2 IRC

    #redis on irc.perl.org
    
=head1 DEVELOPMENT

=head2 Repository

    http://github.com/und3f/protocol-redis

=head1 AUTHOR

Sergey Zasenko, C<undef@cpan.org>.

=head1 CREDITS

David Leadbeater (dgl)

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011, Sergey Zasenko.

This program is free software, you can redistribute it and/or modify it under
the same terms as Perl 5.10.

=cut
