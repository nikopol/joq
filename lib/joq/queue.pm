package joq::queue;

use strict;
use warnings;
use 5.010;

use Time::HiRes qw(sleep);
use joq::logger;
use joq::job;

my %jobs;
my @runs;
my @readys;
my @history;
my $polling;
my $runcount = 0;
my $timeoutcount = 0;
my $paused = 0;
my $alone = 0;

our $pollend = 0;

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
		log::info($key.' set to '.$cfg{$key});
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

sub jobbypid {
	my $pid = shift;
	my( $job ) =
		grep { $_->{pid} == $pid }
		map { $jobs{$_} }
		@runs;
	$job
}

sub jobbyname {
	if( my $name = shift ) {
		my( $job ) =
			grep { $_->{name} && $_->{name} eq $name }
			values %jobs;
		return $job;
	}
	undef;
}

sub jobids {
	keys %jobs
}

sub jobs {
	values %jobs
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

sub runcount {
	scalar @runs
}

sub addjobs {
	my $newjobs = shift;
	return 0 unless $newjobs;
	$newjobs = [ $newjobs ] unless ref($newjobs) eq 'ARRAY';
	my $adds = [];
	log::debug('queuing '.@$newjobs.' jobs');
	foreach( @$newjobs ) {
		my $job = addjob( $_, 0 );
		push( @$adds, { id => $job->{id}, fullname => $job->{fullname} } ) if $job;
	}
	$adds;
}

sub addjob {
	my $jobarg = shift;
	return undef unless $jobarg;
	my $job = joq::job::setup( $jobarg );
	if( $job ) {
		if( !exists $jobs{$job->{id}} && !jobbyname($job->{name}) ) {
			$jobs{$job->{id}} = $job;
			log::debug($job->{fullname}.' queued');
		} else {
			log::error($job->{fullname}.' already queued, ignored');
			$job = undef;
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
	sleep 0.1 while( $count-- && joq::job::running( $job, 1 ) );
	if( joq::job::running( $job, 1 ) ) {
		log::info('kill '.$job->{fullname}.' (still running after stop)');
		joq::job::kill( $job );
		joq::job::finished( $job );
	} else {
		log::info($job->{fullname}.' softly stopped');
	}
	$alone = 0 if $job->{when}{alone} && !joq::job::running( $job );
	1;
}

sub deljob {
	return 0 unless my $job = job( shift );
	stopjob( $job );
	@readys = grep { $_ != $job->{id} } @readys;
	@runs = grep { $_ != $job->{id} } @runs;
	historize( delete $jobs{$job->{id}} );
	1
}

sub killall {
	log::core('killall ! ('.scalar @runs.' jobs running)');
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

sub pause {
	return 0 if $paused;
	log::notice "queue paused";
	$paused = 1;
}

sub resume {
	return 0 unless $paused;
	log::notice "queue resumed";
	$paused = 0;
	1
}

sub status {
	+{
		maxfork      => $cfg{maxfork},
		status       => $paused?'paused':'running',
		jobs_queued  => scalar keys %jobs,
		jobs_dead    => scalar @history,
		jobs_running => scalar @runs,
		jobs_run     => $runcount,
		jobs_ready   => scalar @readys,
		jobs_timeout => $timeoutcount,
		flag_alone   => $alone,
	};
}

sub poll {
	return undef if $polling;
	my $softstop = shift;
	$polling = 1;
	my %finished;
	my $nbevent = 0;
	if( @runs ) {
		log::debug('polling with '.(keys %jobs).' jobs queued');
		#check 'running' jobs
		my @alive;
		foreach my $jid ( @runs ) {
			my $job = $jobs{$jid};
			my $run = joq::job::running($job);
			if( $run ) {
				log::debug($job->{fullname}.' still running');
				if(joq::job::timeout($job)){
					log::warning($job->{fullname}.' timeout');
					stopjob($job);
					$job->{timeoutcount}++;
					$timeoutcount++;
					undef $run;
				} else {
					push @alive, $jid;
				}
			}
			unless( $run ){
				$finished{$job->{name}} = $jid if exists $job->{name};
				$runcount++;
				if( joq::job::dead($job) ) {
					log::debug($job->{fullname}.' marked as finished and dead');
					historize( delete $jobs{$jid} );
				} else {
					log::debug($job->{fullname}.' marked as finished and pending');
				}
				$alone = 0;
				$nbevent++;
			}
		}
		@runs = @alive;
	}
	my @jobids = keys %jobs;
	if( @jobids && !$softstop ) {
		#check startable jobs, ordered by priority
		my @pending =
			sort { $jobs{$b}->{order} <=> $jobs{$a}->{order} }
			grep { !($_ ~~ @runs) }
			grep { !($_ ~~ @readys) }
			@jobids;
		my $nbrdy = 0;
		foreach my $jid ( @pending ) {
			my $job = $jobs{$jid};
			if( joq::job::startable( $job, \%finished ) ) {
				log::debug($job->{fullname}.' ready to start');
				push @readys, $jid;
				$nbrdy++;
			}
		}
		#runs jobs if fork slot available
		unless( $paused || $alone || @runs >= $cfg{maxfork} ) {
			my @stillreadys;
			while( my $jid = shift @readys ) {
				my $job = $jobs{$jid};
				my $wal = $job->{when}{alone};
				if( $wal && scalar @runs ) {
					push @stillreadys, $jid;
				} elsif(joq::job::start( $job )) {
					push @runs, $jid;
					$nbevent++;
					if( $wal ) {
						log::info($job->{fullname}.' activates alone mode');
						$alone = 1;
						push @stillreadys, @readys;
						last;
					}
					if( @runs >= $cfg{maxfork} ) {
						push @stillreadys, @readys;
						last;
					}
				} else {
					log::error('error starting '.$job->{fullname}.', unqueued');
				}
			}
			@readys = @stillreadys;
		}
		log::debug(
			'polling done.'.
			' running='.scalar @runs.'/'.$cfg{maxfork}.
			' ready='.scalar @readys.'/'.(scalar @jobids - scalar @runs).
			($paused?' (paused)':'').
			($alone?' (alone mode)':'').
			($softstop?' (soft stop)':'')
		) if @jobids;
	}
	$polling = 0;
	$pollend = time;
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
