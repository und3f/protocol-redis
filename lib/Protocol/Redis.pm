package Protocol::Redis;

use strict;
use warnings;

use List::Util ();
require Carp;

our $VERSION = 0.1;

sub new {
    my $class = shift;
    $class = ref $class if ref $class;

    my $self = {@_};
    bless $self, $class;

    $self->newline($self->{newline});
    $self->on_command($self->{on_command});
    $self->{_commands} = [];
    $self->_change_state(\&_state_new_command);

    $self;
}

sub newline {
    my ($self, $newline) = @_;

    $self->{_newline} = $newline or "\r\n";
}

sub get_command {
    my ($self) = @_;
    shift @{$self->{_commands}};
}

sub on_command {
    my ($self, $cb) = @_;
    $self->{_on_command_cb} = $cb;
}

sub parse {
    my ($self, $chunk) = @_;

    # Just pass chunk to current vertex
    $self->{_state}->($self, $chunk);
}

sub _change_state {
    my ($self, $new_state, $chunk) = @_;

    $self->{_state} = $new_state;

    # Pass rest of chunk to new vertex
    $new_state->($self, $chunk) if $chunk;
}

sub _command_parsed {
    my ($self, $chunk) = @_;

    my $command = delete $self->{_cmd};

    if (my $cb = $self->{_on_command_cb}) {
        $cb->($self, $command);
    }
    else {
        push @{$self->{_commands}}, $command;
    }
    $self->_change_state(\&_state_new_command, $chunk);
}

sub _state_new_command {
    my ($self, $chunk) = @_;

    $self->{_cmd} = {type => undef, data => undef};

    my $cmd = substr $chunk, 0, 1, '';

    if (List::Util::first { $cmd eq $_ } (qw/+ - :/)) {
        $self->{_cmd}{type} = $cmd;
        $self->{_state_cb} = \&_command_parsed;
        $self->_change_state(\&_state_string_command, $chunk);
    }
    elsif ($cmd eq '$') {
        $self->{_cmd}{type} = $cmd;
        $self->{_state_cb} = \&_command_parsed;
        $self->_change_state(\&_state_bulk_command, $cmd . $chunk);
    }
    elsif ($cmd eq '*') {
        $self->{_cmd}{type} = $cmd;
        $self->{_state_cb} = \&_command_parsed;
        $self->_change_state(\&_state_multibulk_command, $chunk);

    }
    else {
        Carp::croak(qq/Unexpected input "$cmd"/);
    }
}

sub _state_string_command {
    my ($self, $chunk) = @_;

    my $str = $self->{_state_string} .= $chunk;
    my $i = index $str, $self->newline;

    # string isn't full
    return if $i < 0;

    # We got full string
    my $result = substr $str, 0, $i, '';

    # Delete newline
    substr $str, 0, length($self->newline), '';

    delete $self->{_state_string};

    $self->{_cmd}{data} = $result;
    $self->{_state_cb}->($self, $str);
}

sub _state_bulk_command {
    my ($self, $chunk) = @_;

    my $bulk_state_cb = $self->{_state_cb};

    # Read bulk command size
    $self->{_state_cb} = sub {
        my ($self, $chunk) = @_;

        $self->{_bulk_size} = delete $self->{_cmd}{data};

        # Delete starting '$'
        substr $self->{_bulk_size}, 0, 1, "";

        if ($self->{_bulk_size} == -1) {
            # Nil
            $self->{_cmd}{data} = undef;
            $bulk_state_cb->($self, $chunk);
        } else {
            $self->{_state_cb} = $bulk_state_cb;
            $self->_change_state(\&_state_bulk_command_data, $chunk);
        }
    };
    $self->_change_state(\&_state_string_command, $chunk);
}

sub _state_bulk_command_data {
    my ($self, $chunk) = @_;

    my $str = $self->{_state_string} .= $chunk;
    if (length $str >= $self->{_bulk_size} + length $self->newline) {
        my $result = substr $str, 0, $self->{_bulk_size}, '';

        # Delete ending newline
        substr $str, 0, length $self->newline, '';

        delete $self->{_state_string};
        delete $self->{_bulk_size};

        $self->{_cmd}{data} = $result;
        $self->{_state_cb}->($self, $str);
    }
}

sub _state_multibulk_command {
    my ($self, $chunk) = @_;

    my $mbulk_state_cb = delete $self->{_state_cb};
    my $data           = [];
    my $mbulk_process;

    $mbulk_process = sub {
        my ($self, $chunk) = @_;

        push @$data, delete $self->{_cmd}{data};

        if (scalar @$data == $self->{_mbulk_arg_num}) {

            # Cleanup
            $mbulk_process = undef;
            delete $self->{_mbulk_arg_num};
            delete $self->{_state_cb};

            # Return command
            $self->{_cmd}{data} = $data;
            $mbulk_state_cb->($self, $chunk);
        }
        else {

            # read next string
            $self->{_state_cb} = $mbulk_process;
            $self->_change_state(\&_state_bulk_command, $chunk);
        }
    };

    $self->{_state_cb} = sub {
        my ($self, $chunk) = @_;

        # Number of Multi-Bulk command
        my $num = $self->{_mbulk_arg_num} = delete $self->{_cmd}{data};
        if ($num < 1) {
            $mbulk_process = undef;
            $self->{_cmd}{data} = [];
            $mbulk_state_cb->($self, $chunk);
        }
        else {

            # We got commands
            $self->{_state_cb} = $mbulk_process;
            $self->_change_state(\&_state_bulk_command, $chunk);
        }
    };

    # Get number of commands
    $self->_change_state(\&_state_string_command, $chunk);
}

1;
__END__

=head1 NAME

Protocol::Redis - Redis protocol parser

=head1 DESCRIPTION

Redis protocol parser.

=head1 METHODS

=head2 C<parse>

    $redis->parse("*2\r\n$4ping\r\n\r\n");

Parse Redis protocol chunk.

=head2 C<get_command>

    while (my $command = $redis->get_command) {
        ...
    }

Get parsed command or undef.

=head2 C<on_command>

    $redis->on_command(sub {
        my ($redis, $command) = @_;

    }

Calls callback on each parsed command.

=head1 AUTHOR

Sergey Zasenko, C<undef@cpan.org>.

=head1 COPYRIGHT (C) 2011, Sergey Zasenko.

This program is free software, you can redistribute it and/or modify it under
the same terms as Perl 5.10.

=cut
