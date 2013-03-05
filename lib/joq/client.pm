package joq::client;

use strict;
use warnings;

use Socket;
use JSON::XS;

use constant MAXLOGLINES => 20;

sub new {
	my $class = shift;
	my %o = @_;
	my $self = {
		host    => 'localhost',
		port    => 1970,
		mode    => 'perl',
		timeout => 10,
		%o,
		sock    => undef,
		error   => "",
		log     => [],
	};
	if( $o{server} ) {
		if( $o{server} =~ /^([^\:]+)\:(\d+)$/ ) {
			$self->{host} = $1;
			$self->{port} = $2;
		} else {
			$self->{host} = $o{server};
		}
	}
	$self->{decode} = $self->{mode} eq 'perl';
	$self->{imode}  = $self->{decode} ? 'json' : $self->{mode};
	bless $self, $class;
	my $r = $self->connect;
	print "connecting to ".$self->{host}.":".$self->{port}." ... ".($r||$self->{error})."\n"
		if $self->{debug};
	$self;
}

sub error {
	my( $self, $err ) = @_;
	return $self->{error} unless $err;
	warn "ERROR: $err\n" if $self->{debug};
	$self->{error} = $err;
	0;
}
	
sub connected {
	my $self = shift;
	$self->{sock} ? 1 : 0;
}

sub connect {
	my $self = shift;
	return 0 if $self->{sock};
	my $iadd = inet_aton( $self->{host} ) or return $self->error("host not found");
	my $padd = sockaddr_in( $self->{port}, $iadd );
	my $prot = getprotobyname( 'tcp' );
	my $sock;
	socket( $sock, PF_INET, SOCK_STREAM, $prot ) or return $self->error("can't open socket");
	connect( $sock, $padd ) or return $self->error("can't connect to ".$self->{host}.":".$self->{port});
	$self->{sock} = $sock;
	$self->read();
	$self->mode( $self->{imode} ) if $self->{imode};
}

sub read {
	my $self = shift;
	my $buf  = '';
	$SIG{ALRM} = sub { die "read timeout\n"; };
	alarm $self->{timeout};
	while( $buf !~ /\>$/ ) {
		sysread( $self->{sock}, $buf, 262143, length($buf) ) || return $self->error('connection closed');
	}
	alarm 0;
	my @lines = split(/\n/, $buf);
	map { chomp } @lines;
	if( $self->{debug} ) { map { print "RECV <= $_\n" } @lines; }
	my $nb = @lines;
	pop @lines if $nb && $lines[$nb-1] eq '>';
	$self->log( @lines );
	\@lines;
}

sub log {
	my( $self, @lines ) = @_;
	return (wantarray?@{$self->{log}}:join("\n",@{$self->{log}})) unless @lines;
	push @{$self->{log}}, @lines;
	shift @{$self->{log}} while( @{$self->{log}} > MAXLOGLINES );
	scalar @{$self->{log}};
}

sub disconnect {
	my $self = shift;
	close( $self->{sock} ) if  $self->{sock};
	1;
}

sub cmd {
	my $self = shift;
	my $cmd  = shift;
	$cmd .= ' '.(ref($_[0]) eq 'HASH' ? encode_json($_[0]) : join(' ',@_)) if @_;
	$cmd .= "\n";
	return 'not connected' unless $self->{sock};
	print "SEND => $cmd" if $self->{debug};
	syswrite $self->{sock}, '<'.length($cmd).'>' || return $self->error("write error");
	syswrite $self->{sock}, $cmd || return $self->error("write error");
	my $read = $self->read;
	my $r = $read ? join("\n", @$read) : $self->error;
	$self->{decode} ? decode_json( $r ) : $r;
}

sub mode { shift->cmd('mode',@_) }
sub load { shift->cmd('load',@_) }
sub list { shift->cmd('list') }
sub show { shift->cmd('show',@_) }
sub add  { shift->cmd('add',@_) }
sub del  { shift->cmd('del',@_) }
sub stop { shift->cmd('stop',@_) }
sub save { shift->cmd('save',@_) }
sub killall  { shift->cmd('killall') }
sub shutdown { shift->cmd('shutdown') }
sub history  { shift->cmd('history',@_) }
sub status   { shift->cmd('status') }
sub pause    { shift->cmd('pause') }
sub remuse   { shift->cmd('resume') }

1;
__END__

=head1 NAME

joq - Client

=head1 SYNOPSIS

  use joq::client;

  my $cli = joq::client->new(
  	host => 127.0.0.1,
	port => 1970,
	mode => 'text' #could be yaml,json or text
  );
  print( $cli->status() || $cli->error );


  #get joq status
  print joq::client->new->status;

  #send a job - first way
  joq::client->new->add('code while(1){ print "bazinga\n"; sleep 10; } name=bazinga delay=30');
  joq::client->new->add('shell echo prout name=petoman repeat=3');

  #send jobs - second way
  joq::client->new->load({
    jobs => [
      { shell => 'echo "bazinga"', name => 'bazinga', when => { delay => 30 }},
      { class => 'My::Class' },
    ]
  });

  #list queued jobs
  print joq::client->new->list;

  #show detail on a job or id
  print joq::client->new->show('bazinga');
  print joq::client->new->show(1);

  #del a job by its name or id
  joq::client->new->del('bazinga');
  joq::client->new->del(2);

  #stop a running job or id
  joq::client->new->stop('bazinga');
  joq::client->new->stop(3);

  #killall running jobs
  joq::client->new->killall;

  #shutdown joq server
  joq::client->new->shutdown;

=head1 DESCRIPTION

=head1 AUTHOR

E<lt>nikomomo@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
