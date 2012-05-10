package joq::remote;

use warnings;
use strict;

use joq::logger;
use joq::client;

my $oid = 0;
my %joqs;
my $syncache;

sub add {
	my $arg = shift;
	return 0 unless $arg;
	my( $name, $host, $port, $sync );
	foreach( split / /,$arg ) {
		if( /^\s*([^\:]+)\:(\d+)\s*$/ ) {
			($host,$port) = ($1,$2);
			my @doms = split /\./,$host;
			$name = $doms[0] unless $name;
		} elsif( /sync/i ) {
			$sync = 1;
		} else {
			$name = $_;
		}
	}
	if( $host && $port ) {
		my $k = "$host:$port";
		return 0 if exists $joqs{$k};
		$joqs{$k} = {
			name => $name,
			host => $host,
			port => $port,
			sync => $sync,
			logn => '@'.uc($name),
			pid  => 'S'.(++$oid),
		};
		push @$syncache, $k;
		log::notice("$name $k added");
		return $joqs{$k};
	}
	0;
}

sub del {
	my @l = find( shift );
	foreach( @l ) {
		delete $joqs{$_};
		log::notice("$_ removed");
	}
	$syncache = undef if @l;
	scalar @l;
}

sub sync {
	my @l = grep { !$joqs{$_}->{sync} } find( shift );
	foreach( @l ) {
		$joqs{$_}->{sync} = 1;
		push @$syncache, $_;
		log::notice("$_ synced");
	}
	$syncache = undef if @l;
	@l;
}

sub unsync {
	my @l = grep { $joqs{$_}->{sync} } find( shift );
	foreach( @l ) {
		$joqs{$_}->{sync} = 0;
		log::notice("$_ unsynced");
	}
	$syncache = undef if @l;
	@l;
}

sub synced {
	$syncache = [ grep { $joqs{$_}->{sync} } keys %joqs ] unless $syncache;
	@$syncache;
}

sub find {
	my $arg = shift;
	return () unless $arg;
	return keys %joqs if $arg eq 'all';
	my @l;
	foreach( keys %joqs ) {
		my $s = $joqs{$_};
		push @l, $_ if $s->{name} =~ $arg;
	}
	@l;
}

sub exec {
	my( $cmd, $list, $fh ) = @_;
	my $nb = 0;
	foreach( @$list ) {
		if( my $s = $joqs{$_} ) {
			eval {
				log::debug("sending $cmd", $s->{logn}, $s->{pid});
				$s->{log} = joq::client->new(
					host=>$s->{host},
					port=>$s->{port},
					mode=>'json',
				)->cmd( $cmd );
				my @outs = split /\n/,  $s->{log};
				foreach( @outs ) {
					log::info($_, $s->{logn}, $s->{pid});
					syswrite $fh, $s->{logn}.': '.$_."\n" if $fh;
				}
				$nb++;
			};
			log::error("error sending command to $_ : $@", $s->{logn}, $s->{pid})
				if $@;
		} else {
			log::error("unable to send cmd to $_, server not found")
		}
	}
	$nb;
}


1;
__END__

=head1 NAME

joq::remote -

=head1 SYNOPSIS

  use joq;

  joq::run;

=head1 DESCRIPTION

joq is

=head1 AUTHOR

niko E<lt>nikomomo@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
