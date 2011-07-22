package joq::job;

use strict;
use warnings;

use POSIX qw(:sys_wait_h);

use AnyEvent;
use DateTime;
use Try::Tiny;

use joq;
use joq::logger;

use constant {
	DAYSEC      => 86400, #24*60*60
	STDOUTLINES => 50,
	STDERRLINES => 50,
};

my $gid = 0;

our %cfg = (
	timezone => $ENV{TZ} || 'Europe/Paris',
);

sub config {
	my( $key, $val ) = @_;
	return \%cfg unless $key;
	return undef unless exists $cfg{$key};
	if( defined $val ) {
		$cfg{$key} = $val;
		log::notice($key.' set to '.$val);
		$log::TZ = $val if $key eq 'timezone';
	} else {
		$val = $cfg{$key};
	}
	$val;
}

sub setup {
	# name     = "jobname"
	# args     = @args given to sub as arguments if present
	# shell    = cmd
    # nice     = int -20(fast) > 19(slow)
	# class    = class::name with ->new and ->run(@args) methods
    # package  = class::name to use
	# code     = code_to_eval
	# logfile  = log filename
    # priority = 1-10 (1=slow,10=speed,default=5) 
	# if       = code_to_eval
	# onfinish = sub (internal usage)
	# when = {
	#   dayofweek:"all|sat,mon,1-7" 1=monday, time:"hh:mm,hh:mm,.."
	#   dayofmonth:"all|1-31,...", time:"hh:mm,hh:mm,..."
	#   dayofyear:"all|1-365,...", time:"hh:mm,hh:mm,..."
	#   repeat:4s|3m|2h|1d
	#   after:"jobname"
	#   start:"yyyy-mm-dd hh:mm"
	#   delay:4s|3m|2h|1d
	#   count:nbtime
	# } default={ count:1 }

	my $args = shift || {};
	my $job = {
		priority => 5,
	 	%$args,
		runcount => 0,
		pid      => 0,
		lastout  => [],
		lasterr  => [],
		laststart=> 0,
		lastend  => 0,
		exitcode => undef,
		afterdone=> {},
	};
	unless( $job->{shell} || $job->{class} || $job->{code} ) {
		log::warn("you must give a shell, class or code to run");
		return undef;
	}
	$gid++;
	$gid = 1 if $gid > 99999;
	$job->{id} = $gid;
	$job->{order} = ($job->{priority} * 100000) - $job->{id};
	if( $job->{when} ) {
		$job->{when}->{start} = e2date( time + delay2sec($job->{when}->{delay}) ) if $job->{when}->{delay};
		$job->{when}->{dayofweek}  = delete $job->{when}->{dow} if $job->{when}->{dow};
		$job->{when}->{dayofmonth} = delete $job->{when}->{dom} if $job->{when}->{dom};
		$job->{when}->{dayofyear}  = delete $job->{when}->{doy} if $job->{when}->{doy};
		$job->{fixeday} = 0+
			defined($job->{when}->{dayofweek})+ 
			defined($job->{when}->{dayofmonth})+
			defined($job->{when}->{dayofyear});
		if( defined $job->{when}->{time} && !$job->{fixeday} ) {
			$job->{fixeday} = 1;
			$job->{when}->{dayofweek} = 'all';
		}
		if( $job->{fixeday} ) {
			if( $job->{fixeday} > 1 ) {
				log::warn("wrong dayof... definition");
				return undef;
			}
			#normalize 'time'
			$job->{when}->{time} = ['00:00:00'] unless defined $job->{when}->{time};
			my @times = ref($job->{when}->{time}) eq 'ARRAY' ? @{$job->{when}->{time}} : split /[, ]/,$job->{when}->{time};
			my @ntimes;
			my $delta = DateTime->now( time_zone=>$cfg{timezone} )->offset;
			foreach( @times ) {
				my($h,$m,$s) = split /[\:hm]/, $_;
				$h = 3600 * ($h || 0);
				$m = 60 * ($m || 0);
				$s = 0 + ($s || 0);
				push @ntimes, $h+$m+$s-$delta;
			}
			my @stimes = sort @ntimes;
			$job->{when}->{ntime} = \@stimes;
			#log::debug('times='.join(',',@stimes));
			#normalize 'dayofweek'
			my $okday = 0;
			if( my $dow = $job->{when}->{dayofweek} ) {
				$dow = [ split(/[, ]/,$dow) ] unless ref($dow) eq 'ARRAY';
				my @days = (
					 "^(sun|dim)", "^(mon|lun)", "^(tue|mar)", "^(wed|mer)",
					 "^(thu|jeu)", "^(fri|ven)", "^(sat|sam)",
				);
				my @jobdays=(0,0,0,0,0,0,0);
				foreach my $d ( @$dow ) {
					my $n;
					if( $d =~ /^[1-7]+$/ ) {
						$n = (0+$d) % 7; #7=0=sunday
					} elsif( $d =~ /^(\*|all)$/i )  {
						$okday = 7;
						@jobdays=(1,1,1,1,1,1,1);
						last;
					} else {
						for(my $i = 0; $i < scalar @days; ++$i) {
							if( $d =~ qr/$days[$i]/i ) {
								$n = $i;
								last;
							}
						}
					}
					unless( defined $n && $n<7 ) {
						log::warn('unrecognized day of week '.$d.' ignored');
						next;
					}
					$jobdays[$n] = 1;
					$okday++;
				}
				$job->{when}->{ndayofweek} = \@jobdays;
				#log::debug('dayofweek='.join('',@jobdays));
			#normalize 'dayofmonth' / 'dayofyear'
			} else {
				my( $days, $max, $inc );
				if( $job->{when}->{dayofmonth} ) {
					$inc  = 0; 
					$max  = 31;
					$days = $job->{when}->{dayofmonth};
				} else {
					$inc  = 1;
					$max  = 365;
					$days = $job->{when}->{dayofyear};
				}
				$days = [ split(/[, ]/,$days) ] unless ref($days) eq 'ARRAY';
				my @jobdays = map { 0 } ( 0 .. $max );
				foreach my $d ( @$days ) {
					if( $d =~ /^(\*|all)$/i ) {
						$okday = $max;
						@jobdays = map { 1 } ( 0 .. $max );
						last;
					}
					my $n;
					$n = $d if $d =~ /^[0-9]+$/;
                    unless( defined $n && $n>0 && $n<=$max ) {
                        log::warn("unrecognized day $d of month/year, ignored");
                        next;
                    }
                    $jobdays[$n-$inc] = 1;
					$okday++;
				}
				if( $job->{when}->{dayofmonth} ) {
					$job->{when}->{ndayofmonth} = \@jobdays;
					#log::debug('dayofmonth='.join('',@jobdays));
				} else {
					$job->{when}->{ndayofyear} = \@jobdays;
					#log::debug('dayofyear='.join('',@jobdays));
				}
			}
			unless( $okday ) {
				log::warn('empty dayof..., job ignored');
				return undef;
			}
		}
		$job->{when}->{count} = 1 unless $job->{fixeday} || $job->{when}->{after} || $job->{when}->{repeat} || defined($job->{when}->{count});
	} else {
		$job->{when} = { count=>1 }; #one shot by default
	}
	$job->{when}->{start} = calcnextstart( $job ) unless $job->{when}->{start};
	$job->{fullname} = 'job#'.$job->{id}.($job->{name}?' ['.$job->{name}.']':'');
	log::notice($job->{fullname}.' of type '.($job->{code}?'code':$job->{class}?'class':'shell').' gonna start '.nextstart($job));
	$job;
}

sub start {
	my $job = shift;
	return 0 if running( $job );

	log::debug($job->{fullname}.' starting');

	$job->{lastout} = [];
	$job->{lasterr} = [];
	$job->{exitcode} = undef;

	pipe my $readout, my $writout;
	pipe my $readerr, my $writerr;
	my $pid = fork;

	return 0 unless defined $pid;

	if( $pid ) {
		close $writout;
		close $writerr;
		$job->{pid} = $pid;
		$job->{runcount}++;
		$job->{fullname} .= ' pid='.$pid;

		#my $wchild; $wchild = AnyEvent->child(
		#	pid => $pid, 
		#	cb  => sub {
		#		my( $pid, $status ) = @_;
		#		log::debug('SIGCHLD! '.$job->{fullname}.' finishing with '.$status);
		#		finish( $job );
		#		undef $wchild;
		#	}
		#);
	
		if( $job->{logfile} ) {
			open( $job->{logfh}, '>>', $job->{logfile} )
				or log::error('unable to open '.$job->{fullname}.' logfile '.$job->{logfile});
			filog( $job, 'joq', $job->{fullname}.' started' );
		}

		my( $wout, $werr ); 
		$wout = AnyEvent->io(
			fh   => $readout,
			poll => 'r',
			cb   => sub {
				my $r = sysread $readout, my $buf, 4096;
				if( $r ) {
					my @lines = split /\n/, $buf;
					push @{$job->{lastout}}, @lines;
					shift @{$job->{lastout}} while( @{$job->{lastout}} > STDOUTLINES );
					filog($job,'OUT',@lines);
					log::notice($_,undef,$job->{pid}) foreach @lines;
				} elsif( $r <= 0 ) {
					log::debug(($r==0?'end of':'broken').' STDOUT pipe! '.$job->{fullname}.($werr?'':' finishing'));
					finished( $job ) unless $werr;
					close $readout;
					undef $wout;
				}
			},
		);
		$werr = AnyEvent->io(
			fh   => $readerr,
			poll => 'r',
			cb   => sub {
				my $r = sysread $readerr, my $buf, 4096;
				if( $r ) {
					my @lines = split /\n/, $buf;
					push @{$job->{lasterr}}, @lines;
					shift @{$job->{lasterr}} while( @{$job->{lasterr}} > STDERRLINES );
					filog($job,'ERR',@lines);
                    log::error($_,undef,$job->{pid}) foreach @lines;
                } elsif( $r <= 0 ) {
                    log::debug(($r==0?'end of':'broken').' STDERR pipe! '.$job->{fullname}.($wout?'':' finishing'));
                    finished( $job ) unless $wout;
					close $readerr;
					undef $werr;
                }
            },
        );
		$job->{laststart} = time;
		$job->{lastend} = undef;
		log::notice($job->{fullname}.' started');
		return $job->{pid};
	}

	#child code

	#($EUID, $EGID) = ($UID, $GID);
	close $readout;
	close $readerr;
	open STDOUT, '>&', $writout;
	open STDERR, '>&', $writerr;

	setpriority(0,0,$job->{nice}) if $job->{nice};
	joq::stopevents();
	sleep 1; #wait for parent event hook

	my $e = 0;
	if( $job->{code} ) {

		no warnings;
		no strict;
		
		try {
			$e = 0+eval($job->{code});
		} catch {
			warn "CODE ERROR: $_\n";
			$e = 255;
		}

	} elsif( $job->{class} ) {

		try {
			my $o;
			if( eval "require ".($job->{package} || $job->{class}) ) {
				if( my $o = eval($job->{class}."->new") ) {
					$e = $o->run($job->{args});
				} else {
					warn "CLASS ERROR: unable to call ".$job->{class}."->new\n";
					$e = 254;
				}
			} else {
				warn "CLASS ERROR: package ".($job->{package} || $job->{class})." not found\n";
				$e = 253;
			}
		} catch {
			warn "CLASS ERROR: package $_\n";
			$e = 255;
		}

	} elsif( $job->{shell} ) {

		my @args = ( $job->{shell} );
		if( $job->{args} ) {
			if( ref($job->{args}) eq 'ARRAY' ) {
				push @args, @{$job->{args}};
			} else {
				push @args, $job->{args};
			}
		}
		$e = system @args;
	
	}
	exit $e;
}

sub filog {
	my $job = shift;
	return 0 unless my $h = $job->{logfh};
	my $typ = shift;
	my $tim = e2date(time);
	foreach( @_ ) {
		print $h ($tim,'|',$typ,'|',$_,"\n");
	}
	1;
}

sub duration {
	my $job = shift;
	return "n/a" unless $job->{laststart} && $job->{lastend};
	my $s = $job->{lastend} - $job->{laststart};
	my $m = int($s / 60);
	my $h = int($m / 60);
	sprintf("%dh%02dm%02ds", $h, $m%60, $s%60);
}

sub e2date {
	my $e = shift;
	return 'n/a' unless $e;
	my $d = DateTime->from_epoch( epoch=>$e, time_zone=>$cfg{timezone} );
	$d->ymd.' '.$d->hms;
}

sub delay2sec {
	my $d = shift;
	my $s = 0;
	if( $d ) {
		$s += 60*60*24*$1 if $d =~ s/(\d+)[dj]//i;
		$s += 60*60*$1    if $d =~ s/(\d+)h//i;
		$s += 60*$1       if $d =~ s/(\d+)m//i;
		$s += 0+$1        if $d =~ m/(\d+)/;
	}
	$s
}

sub finished {
	my( $job, $exitcode ) = @_;
	return 0 unless $job->{pid};
	$job->{when}->{count}-- if defined $job->{when}->{count};
	$job->{lastend} = time;
	$job->{afterdone} = {};
	$job->{when}->{start} = calcnextstart( $job );
	if( $exitcode ) {
		$job->{exitcode} = $exitcode;
	} else {
		waitpid $job->{pid}, 0;
		$job->{exitcode} = $? >> 8;
	}
	my $l = $job->{fullname}.' finished in '.duration($job).' with exit code '.$job->{exitcode};
	log::notice($l);
	filog($job,$l);
	$l = $job->{fullname}.' next start '.nextstart( $job );
	log::info($l);
	if( $job->{logfh} ) {
		filog($job,'joq',$l,'----------------------------');
		close delete $job->{logfh};
	}
	$job->{pid} = 0;
	$job->{fullname} =~ s/ pid=\d*//;
	kill 12, $$; #SIGUSR2 => joq::Queue::poll
	1;
}

sub running {
	my $job = shift;
	return 0 unless $job->{pid};
	my $r = waitpid( $job->{pid}, WNOHANG );
	my $c = $? >> 8;
	if( $r > 0 ) {
		#terminated
		log::debug($job->{fullname}.' pid terminated');
		finished( $job, $c );
	} elsif( $r == -1 ) {
		#not exists ?!
		log::debug($job->{fullname}.' pid don\'t exists');
		finished( $job, $c );
	}
	$job->{pid};
}

sub stop {
	my $job = shift;
	return undef unless my $jid = running( $job );
	log::debug($job->{fullname}.' send int signal');
	kill 2, $jid;
}

sub kill {
	my $job = shift;
	return undef unless my $jid = running( $job );
	log::debug($job->{fullname}.' send kill signal');
	kill 9, $jid;
}

sub calcnextstart {
	my $job = shift;
	return undef if defined($job->{when}->{count}) && $job->{when}->{count}<1;
	my $last = shift || $job->{laststart};
	if( $job->{when}->{repeat} ) {
		my $e = $last ? $last + delay2sec($job->{when}->{repeat}) : time;
		return e2date( $e );
	} elsif( $job->{fixeday} ) {
		my $now = time;
		$last ||= $now;
		my @times = @{$job->{when}->{ntime}};
		my $nbtimes = scalar @times;
		my $e = $last;
		$e -= DAYSEC;
		my $n;
		do {
			#get day
			my( $s,$m,$h,$day,$month,$year,$weekday,$yearday,$isdst );
			do {
				$e += DAYSEC;
				$e -= $e % DAYSEC; #trunc to 0h00m00
				($s,$m,$h,$day,$month,$year,$weekday,$yearday,$isdst) = localtime($e);
			} until(
				($job->{when}->{ndayofweek} && $job->{when}->{ndayofweek}->[$weekday]) ||
				($job->{when}->{ndayofmonth} && $job->{when}->{ndayofmonth}->[$day]) ||
				($job->{when}->{ndayofyear} && $job->{when}->{ndayofyear}->[$yearday])
			);
			#get time
			$n = 0;
			$n++ while( $n < $nbtimes && $times[$n] + $e < $last );
		} while( $n >= $nbtimes );
		$e += $times[$n];
		return e2date( $e );
	}
	undef;
}

sub nextstart {
	my $job = shift;
	return 'never (count over)' if exists($job->{when}->{count}) && $job->{when}->{count} < 1;
	return 'at '.$job->{when}->{start} if $job->{when}->{start};
	return 'after '. $job->{when}->{after} if  $job->{when}->{after};
	return 'never' if dead( $job );
	return 'asap, '.$job->{when}->{count}.' run remains' if $job->{when}->{count};
	return 'asap';
}

sub startable {
	my( $job, $jobend ) = @_;
	return 0 if dead( $job );
	if( defined $job->{when}->{start} ) {
		my $d = e2date( time );
		return 0 if(($d cmp $job->{when}->{start}) < 0);
	}
	if( $job->{when}->{if} ) {
		unless( eval($job->{when}->{if}) ) {
			log::debug $job->{fullname}.' dont pass its if condition';
			return 0;
		}
		log::debug  $job->{fullname}.' validate its if condition';
	}
#	return 0 if $job->{when}->{if} && ! eval($job->{when}->{if});
	if( $job->{when}->{after} ) {
		my $ok = 0;
		foreach my $or ( split /[|]| or /i, $job->{when}->{after} ) {
			my $okand = 1;
			foreach my $and ( split /[&+]| and /i, $or ) {
				$and =~ s/^\s+//;
				$and =~ s/\s+$//;
				$job->{afterdone}->{$and} = $jobend->{$and} if $jobend->{$and};
				$okand &= exists $job->{afterdone}->{$and};
			}
			$ok |= $okand;
		}
		return $ok;
	}
	1;
}

sub dead {
	my $job = shift;
	defined $job->{when}->{count} && $job->{when}->{count}<1;
}

1;
__END__

=head1 NAME

joq - Job

=head1 SYNOPSIS

  use joq::job;

=head1 DESCRIPTION

=head1 AUTHOR

E<lt>nikomomo@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
