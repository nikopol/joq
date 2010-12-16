package joq::queue;

use strict;
use warnings;

use Time::HiRes qw( sleep );

use joq::logger;
use joq::job;

my %jobs;
my @runs;
my @readys;
my @history;
my $polling;
my $runcount = 0;

our %cfg = (
	maxfork     => 4,  #max simultaneous running
	maxhistory  => 16, #max done jobs kept
	termtimeout => 3,  #sec after a term before kill
);

sub config {
	my( $key, $val ) = @_;
	return \%cfg unless $key;
	return undef unless exists $cfg{$key};
	if( defined $val ) {
		$cfg{$key} = 0+$val;
		if( $key eq 'maxhistory' ) {
			$cfg{$key} = 0 if 0+$val < 0;
			histurn();
		}
		log::notice($key.' set to '.$cfg{$key});
	} else { 
		$val = $cfg{$key};
	}
	$val;
}

sub job {
	my $arg = shift;
	return undef unless $arg;
	return $arg if ref($arg) eq 'HASH';
	return $jobs{$arg} if exists $jobs{$arg};
	my $qr = qr/$arg/i;
	foreach( keys %jobs ) {
		my $job = $jobs{$_};
		return $job if $job->{name} && $job->{name} =~ $qr;
	}
	undef;
}

sub jobids {
	keys %jobs;
}

sub jobs {
	values %jobs;
}

sub deadjobs {
	my $arg = shift;
	return @history unless $arg && $arg ne 'all';
	my $qr = qr/$arg/i;
	grep { $_->{id} eq $arg || ($_->{name} && $_->{name} =~ $qr) } @history;
}

sub running {
	my $job = job( shift );
	return 0 unless $job;
	grep { $_ eq $job->{id} } @runs;
}

sub addjobs {
	my $newjobs = shift;
	return 0 unless $newjobs;
	$newjobs = [ $newjobs ] unless ref($newjobs) eq 'ARRAY';
	my $adds = [];
	log::info('queuing '.@$newjobs.' jobs');
	foreach( @$newjobs ) {
		my $job = addjob( $_, 0 );
		push( @$adds, { id => $job->{id}, fullname => $job->{fullname} } ) if $job;
	}
	$adds;
}

sub addjob {
	my $jobarg = shift;
	return 0 unless $jobarg;
	my $job = joq::job::setup( $jobarg );
	if( $job ) {
		if( !exists $jobs{$job->{id}} ) {
			$jobs{$job->{id}} = $job;
			log::info($job->{fullname}.' queued');
		} else {
			log::error($job->{fullname}.' already queued, ignored');
		}
	}
	$job;
}

sub stopjob {
	return 0 unless my $job = job( shift );
	return 0 unless joq::job::running( $job );
	joq::job::stop( $job );
	log::info('stop '.$job->{fullname});
	my $count = $cfg{termtimeout} * 10;
	sleep 0.1 while( $count-- && joq::job::running( $job ) );
	if( joq::job::running( $job ) ) {
		joq::job::kill( $job );
		log::notice('kill '.$job->{fullname}.' (still running after stop)');
	} else {
		log::notice($job->{fullname}.' softly stopped');
	}
	1;
}

sub deljob {
	return 0 unless my $job = job( shift );
	stopjob( $job );
	@readys = grep { $_ != $job->{id} } @readys;
	@runs = grep { $_ != $job->{id} } @runs;
	historize( delete $jobs{$job->{id}} );
	1;
}

sub killall {
	log::notice('killall !');
	joq::job::stop( $jobs{$_} ) foreach( @runs );
	my $count = $cfg{termtimeout} * 10;
	sleep 0.1 while( $count-- && grep { joq::job::running( $jobs{$_} ) } @runs );
	joq::job::kill( $jobs{$_} ) foreach( @runs );
	my @jobids = keys %jobs;
	historize( delete $jobs{$_} ) foreach( @jobids );
	@readys = ();
	@runs = ();
	scalar @jobids;
}

sub historize {
	my $job = shift;
	push @history, $job;
	log::debug($job->{fullname}.' historized ('.histurn().')');
	scalar @history;
}

sub histurn {
	shift( @history ) while( $cfg{maxhistory} < @history );
	scalar @history;
}

sub status {
	{
		maxfork     => $cfg{maxfork},
		queuedjobs  => scalar keys %jobs,
		deadjobs    => scalar @history,
		runningjobs => scalar @runs,
		runnedjobs  => $runcount,
		readyjobs   => scalar @readys,
	};
}

sub poll {
	return undef if $polling;
	$polling = 1;
	my @jobids = keys %jobs;
	my $nbevent = 0;
	if( @jobids ) {
		log::debug('polling with '.(keys %jobs).' jobs queued');
		#check 'running' jobs
		my %finished;
		my @alive;
		foreach my $jid ( @runs ) {
			my $job = $jobs{$jid};
			if( joq::job::running($job) ) {
				log::debug($job->{fullname}.' still running');
				push @alive, $jid;
			} else {
				$finished{$job->{name}} = $jid if exists $job->{name};
				$runcount++;
				if( joq::job::dead($job) ) {
					log::info($job->{fullname}.' marked as finished and dead');
					historize( delete $jobs{$jid} );
					@jobids = keys %jobs;
				} else {
					log::info($job->{fullname}.' marked as finished and pending');
				}
				$nbevent++;
			}
		}
		@runs = @alive;
		#check 'runnable' jobs
		my $readyed = @readys;
		foreach my $jid ( @jobids ) {
			unless( (grep { $_ == $jid } @readys) || (grep { $_ == $jid } @runs) ) {
				my $job = $jobs{$jid};
				if( joq::job::startable( $job, \%finished ) ) {
					log::info($job->{fullname}.' ready to start');
					push @readys, $jid;
				}
			}
		}
		if( $readyed != @readys ) {
			#manage priority
			@readys = sort { $jobs{$b}->{order} <=> $jobs{$a}->{order} } @readys;
		}
		#runs jobs if fork slot available
		while( @runs < $cfg{maxfork} && ( my $jid = shift @readys ) ) {
			my $job = $jobs{$jid};
			if(joq::job::start( $job )) {
				push @runs, $jid;
				$nbevent++;
			} else {
				log::error('error starting '.$job->{fullname}.', unqueued');
			}
		}
		log::debug('polling finish with '.scalar @runs.'/'.$cfg{maxfork}.' jobs running, '.scalar @readys.' ready');
	}
	$polling = 0;
	( scalar @jobids, scalar @runs, $nbevent );
}

1;

__END__

=head1 NAME

joq - queue

=head1 SYNOPSIS

  use joq::queue;

=head1 DESCRIPTION

=head1 AUTHOR

E<lt>nikomomo@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
