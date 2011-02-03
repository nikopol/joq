package joq::client;

use strict;
use warnings;

use Socket;

use constant MAXLOGLINES => 20;
use constant READTIMEOUT => 10;

sub new {
	my $class = shift;
	my %o = @_;
	my $self = {
		host  => 'localhost',
		port  => 1970,
		%o,
		sock  => undef,
		error => "",
		log   => [],
	};
	bless $self, $class;
	my $r = $self->connect;
	print "connecting to ".$self->{host}.":".$self->{port}." ... ".($r||$self->{error})."\n"
		if $self->{debug};
	$self;
}

sub error {
	my( $self, $err ) = @_;
	return $self->{error} unless $err;
	print "ERROR: $err\n" if $self->{debug};
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
	$self->mode( $self->{mode} ) if $self->{mode};
}

sub read {
	my $self = shift;
	my $buf  = '';
	my $timout = 0;
	$SIG{ALRM} = sub { die "read timeout\n"; };
	alarm READTIMEOUT;
	while( $buf !~ /\>$/ ) {
		sysread( $self->{sock}, $buf, 16384, length($buf) ) || return $self->error('connection closed');
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
	my( $self, $cmd ) = @_;
	return 'not connected' unless $self->{sock};
	$cmd .= "\n";
	print "SEND => $cmd" if $self->{debug};
	syswrite $self->{sock}, '<'.length($cmd).'>' || return $self->error("write error");
	syswrite $self->{sock}, $cmd || return $self->error("write error");
	join("\n", @{$self->read});
}

sub mode { shift->cmd('mode '.(shift|'')); }
sub load { shift->cmd('load '.join(' ',@_)); }
sub list { shift->cmd('list'); }
sub show { shift->cmd('show '.join(' ',@_)); }
sub add  { shift->cmd('add '.join(' ',@_)); }
sub del  { shift->cmd('del '.join(' ',@_)); }
sub stop { shift->cmd('stop '.join(' ',@_)); }
sub killall { shift->cmd('killall'); }
sub shutdown { shift->cmd('shutdown'); }
sub history { shift->cmd('history '.(shift||'')); }
sub status { shift->cmd('status'); }

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

  #send jobs - second way
  joq::client->new->load(
    JSON::XS->new->encode({
		jobs => [
			{ shell => 'echo "bazinga"', name => 'bazinga', when => { delay => 30 }},
			{ class => 'My::Class' },
		]
	})
  );

  #list queued jobs
  print joq::client->new->list;

  #show detail on a job
  print joq::client->new->show('bazinga');

  #del a job by its name
  joq::client->new->del('bazinga');

  #stop a running job
  joq::client->new->stop('bazinga');

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
