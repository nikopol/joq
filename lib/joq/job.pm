package joq::job;

use strict;
use warnings;

use POSIX qw(:signal_h :sys_wait_h);

use AnyEvent;
use DateTime;

use joq;
use joq::logger;

use constant {
	DAYSEC      => 86400, #24*60*60
	STDOUTLINES => 50,
	STDERRLINES => 50,

	ENDCHILD    => 1,
	ENDSTDOUT   => 2,
	ENDSTDERR   => 4,
	ENDED       => 7,
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
		log::info($key.' set to '.$val);
		$log::TZ = $val if $key eq 'timezone';
	} else {
		$val = $cfg{$key};
	}
	$val
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
	# onfinish = sub (internal usage)
	# timeout  = 4s|3m|2h|1d
	# when = {
	#   if: start_condition_to_eval
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
		runcount => 0,
		lastout  => [],
		lasterr  => [],
		laststart=> 0,
		lastend  => 0,
		%$args,
		pid      => 0,
		exitcode => undef,
		afterdone=> {},
	};
	unless( $job->{shell} || $job->{class} || $job->{code} ) {
		log::warn("you must give a shell, class or code to run");
		return undef;
	}
	$gid++;
	$gid = 1 if $gid > 999999;
	$job->{id} = $gid;
	$job->{order} = ($job->{priority} * 1000000) - $job->{id};
	if( $job->{when} ) {
		$job->{when}{start} = e2date( time + delay2sec($job->{when}{delay}) ) if $job->{when}{delay};
		$job->{when}{dayofweek}  = delete $job->{when}{dow} if $job->{when}{dow};
		$job->{when}{dayofmonth} = delete $job->{when}{dom} if $job->{when}{dom};
		$job->{when}{dayofyear}  = delete $job->{when}{doy} if $job->{when}{doy};
		$job->{fixeday} = 0+
			defined($job->{when}{dayofweek})+ 
			defined($job->{when}{dayofmonth})+
			defined($job->{when}{dayofyear});
		if( defined $job->{when}{time} && !$job->{fixeday} ) {
			$job->{fixeday} = 1;
			$job->{when}{dayofweek} = 'all';
		}
		if( $job->{fixeday} ) {
			if( $job->{fixeday} > 1 ) {
				log::warn("wrong dayof... definition");
				return undef;
			}
			#normalize 'time'
			$job->{when}{time} = ['00:00:00'] unless defined $job->{when}{time};
			my @times = ref($job->{when}{time}) eq 'ARRAY' ? @{$job->{when}{time}} : split /[, ]/,$job->{when}{time};
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
			$job->{when}{ntime} = \@stimes;
			#log::debug('times='.join(',',@stimes));
			#normalize 'dayofweek'
			my $okday = 0;
			if( my $dow = $job->{when}{dayofweek} ) {
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
				$job->{when}{ndayofweek} = \@jobdays;
			} else {
				my( $days, $max, $inc );
				if( $job->{when}{dayofmonth} ) {
					$inc  = 0; 
					$max  = 31;
					$days = $job->{when}{dayofmonth};
				} else {
					$inc  = 1;
					$max  = 365;
					$days = $job->{when}{dayofyear};
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
				if( $job->{when}{dayofmonth} ) {
					$job->{when}{ndayofmonth} = \@jobdays;
				} else {
					$job->{when}{ndayofyear} = \@jobdays;
				}
			}
			unless( $okday ) {
				log::warn('empty dayof..., job ignored');
				return undef;
			}
		}
		$job->{when}{count} = 1 unless $job->{fixeday} || $job->{when}{after} || $job->{when}{repeat} || defined($job->{when}{count});
	} else {
		$job->{when} = { count=>1 }; #one shot by default
	}
	$job->{when}{start} = calcnextstart( $job ) unless $job->{when}{start};
	$job->{fullname} = 'job#'.$job->{id}.($job->{name}?' ['.$job->{name}.']':'');
	log::debug($job->{fullname}.' of type '.($job->{code}?'code':$job->{class}?'class':'shell').' gonna start '.nextstart($job));
	$job
}

sub start {
	my $job = shift;
	return 0 if running($job);

	log::debug($job->{fullname}.' starting');

	$job->{lastout} = [];
	$job->{lasterr} = [];
	$job->{exitcode} = undef;
	delete $job->{ending};

	pipe my $readout, my $writout;
	pipe my $readerr, my $writerr;
	my $pid = fork;

	if( !defined $pid ){

		log::error "fork error : $@";
		return 0;
	
	} elsif( $pid ) {

		close $writout;
		close $writerr;
		$job->{pid} = $pid;
		$job->{runcount}++;
		$job->{fullname} .= ' pid='.$pid;

		if( $job->{logfile} ) {
			open( $job->{logfh}, '>>', $job->{logfile} )
				or log::error('unable to open '.$job->{fullname}.' logfile '.$job->{logfile});
			filog( $job, 'joq', $job->{fullname}.' started' );
		}

		my( $wout, $werr, $outbuf, $errbuf );
		$outbuf = '';
		$wout = AnyEvent->io(
			fh   => $readout,
			poll => 'r',
			cb   => sub {
				my $r = sysread $readout, my $buf, 4096;
				$buf = $r > 0 ? $outbuf.$buf : $outbuf."\n";
				if( my @lines = split /\n|\r\n/, $buf ) {
					$outbuf = $buf =~ /\n$/ ? '' : pop @lines;
					push @{$job->{lastout}}, @lines;
					shift @{$job->{lastout}} while( @{$job->{lastout}} > STDOUTLINES );
					filog($job,'OUT',@lines);
					log::notice($_,undef,$job->{pid}) foreach @lines;
				}
				if( $r <= 0 ) {
					log::core(($r==0?'end of':'broken').' STDOUT pipe! '.$job->{fullname});
					ending( $job => ENDSTDOUT );
					close $readout;
					undef $wout;
				}
			},
		);
		$errbuf = '';
		$werr = AnyEvent->io(
			fh   => $readerr,
			poll => 'r',
			cb   => sub {
				my $r = sysread $readerr, my $buf, 4096;
				$buf = $r > 0 ? $errbuf.$buf : $errbuf."\n";
				if( my @lines = split /\n|\r\n/, $buf ) {
					$errbuf = $buf =~ /\n$/ ? '' : pop @lines;
					push @{$job->{lasterr}}, @lines;
					shift @{$job->{lasterr}} while( @{$job->{lasterr}} > STDERRLINES );
					filog($job,'ERR',@lines);
                    log::error($_,undef,$job->{pid}) foreach @lines;
                }
                if( $r <= 0 ) {
                    log::core(($r==0?'end of':'broken').' STDERR pipe! '.$job->{fullname});
                    ending( $job => ENDSTDERR );
					close $readerr;
					undef $werr;
                }
            },
        );
		$job->{laststart} = time;
		$job->{lastend} = undef;
		$job->{lastimeout} = time + delay2sec($job->{timeout}) if $job->{timeout};
		log::info($job->{fullname}.' started');
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

	local $SIG{INT} = 'IGNORE';

	my $e = 0;
	if( $job->{code} ) {

		no warnings;
		no strict;
		use 5.010;
		
		$e = 0+eval($job->{code});
		if( $@ ) {
			warn "CODE ERROR: $@\n";
			$e = 255;
		}

	} elsif( $job->{class} ) {

		my $o;
		if( eval "require ".($job->{package} || $job->{class}) ) {
			if( my $o = eval($job->{class}."->new") ) {
				$e = eval { $o->run($job->{args}) };
				if( $@ ) {
					warn "CLASS ERROR: $@\n";
					$e = 255;
				}
			} else {
				warn "CLASS ERROR: unable to call ".$job->{class}."->new\n";
				$e = 254;
			}
		} else {
			warn "CLASS ERROR: package ".($job->{package} || $job->{class})." not found\n";
			$e = 253;
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
		exec join(' ',@args);
	}
	exit( $e ? 0+$e : 0 );
}

sub filog {
	my $job = shift;
	return 0 unless my $h = $job->{logfh};
	my $typ = shift;
	my $tim = e2date(time);
	foreach( @_ ) {
		print $h ($tim,'|',$typ,'|',$_,"\n");
	}
	1
}

sub duration {
	my $job = shift;
	return "n/a" unless $job->{laststart} && $job->{lastend};
	my $s = $job->{lastend} - $job->{laststart};
	my $m = int($s / 60);
	my $h = int($m / 60);
	$h ? sprintf("%dh%02dm%02ds", $h, $m%60, $s%60) :
	$m ? sprintf("%dm%02ds", $h, $m%60, $s%60) :
	$s.'s'
}

sub e2date {
	my $e = shift;
	return 'n/a' unless $e;
	my $d = DateTime->from_epoch( epoch=>$e, time_zone=>$cfg{timezone} );
	$d->ymd.' '.$d->hms
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

sub ending {
	my( $job, $event, $status ) = @_;
	$job->{exitcode} = $status if defined $status;
	$job->{ending} |= $event;
	log::core("job $job->{pid} ending=$job->{ending}");
	finished($job) if $job->{ending} == ENDED;
	$job->{pid}
}

sub finished {
	my( $job ) = @_;
	return 0 unless $job->{pid};
	$job->{when}{count}-- if defined $job->{when}{count};
	$job->{lastend} = time;
	$job->{afterdone} = {};
	$job->{when}{start} = calcnextstart($job);
	$job->{lastduration} = duration($job);
	my $l = $job->{fullname}.' finished in '.$job->{lastduration}.' with exit code '.(defined $job->{exitcode}?$job->{exitcode}:'?');
	log::info($l);
	filog($job,$l);
	$l = $job->{fullname}.' next start '.nextstart($job);
	log::debug($l);
	if( $job->{logfh} ) {
		filog($job,'joq',$l,'----------------------------');
		close delete $job->{logfh};
	}
	$job->{pid} = 0;
	delete $job->{ending};
	$job->{fullname} =~ s/ pid=\d*//;
	kill SIGUSR2 => $$; #poll queue
	1
}

sub running {
	my( $job, $syscheck ) = @_;
	return 0 unless $job->{pid};
	return $job->{pid} unless $syscheck;
	my $r = waitpid( $job->{pid}, WNOHANG );
	my $c = $?;
	if( $r > 0 ) {
		#terminated
		log::debug($job->{fullname}.' pid terminated ('.$c.')');
		finished( $job, $c );
	} elsif( $r == -1 ) {
		#not exists ?!
		log::debug($job->{fullname}.' pid don\'t exists');
		finished( $job );
	}
	$job->{pid}
}

sub timeout {
	my $job = shift;
	$job->{pid} && $job->{lastimeout} && $job->{lastimeout} < time() ? 1 : 0
}

sub stop {
	my $job = shift;
	return undef unless my $jid = running( $job, 1 );
	unless( kill( SIGTERM => $jid ) ) {
		log::core($job->{fullname}.' did not receive term signal');
		return 0;
	}
	log::core($job->{fullname}.' receive term signal');
	1
}

sub kill {
	my $job = shift;
	return undef unless my $jid = running( $job, 1 );
	unless( kill( SIGKILL => $jid ) ) {
		log::core($job->{fullname}.' did not receive kill signal');
		return 0;
	}
	log::core($job->{fullname}.' receive kill signal');
	1
}

sub calcnextstart {
	my $job = shift;
	return undef if defined($job->{when}{count}) && $job->{when}{count}<1;
	my $last = $job->{laststart};
	if( $job->{when}{repeat} ) {
		my $e = $last ? $last + delay2sec($job->{when}{repeat}) : time;
		return e2date( $e );
	} elsif( $job->{fixeday} ) {
		my $now = time;
		$last ||= $now;
		my @times = @{$job->{when}{ntime}};
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
				($job->{when}{ndayofweek}  && $job->{when}{ndayofweek}->[$weekday]) ||
				($job->{when}{ndayofmonth} && $job->{when}{ndayofmonth}->[$day])    ||
				($job->{when}{ndayofyear}  && $job->{when}{ndayofyear}->[$yearday])
			);
			#get time
			$n = 0;
			$n++ while( $n < $nbtimes && $times[$n] + $e <= $last );
		} while( $n >= $nbtimes );
		$e += $times[$n];
		return e2date( $e );
	}
	undef
}

sub nextstart {
	my $job = shift;
	my $w = $job->{when};
	my $a = $w->{alone} ? 'alone ' : '';
	return 'never (count over)' if exists($w->{count}) && $w->{count} < 1;
	return $a.'at '.$w->{start}.', '.$w->{count}.' run remains' if $w->{start} && $w->{count};
	return $a.'at '.$w->{start} if $w->{start};
	return $a.'after '.$w->{after} if $w->{after};
	return 'never' if dead( $job );
	return $a.'asap, '.$w->{count}.' run remains' if $w->{count};
	$a.'asap'
}

sub startable {
	my( $job, $jobend ) = @_;
	return 0 if dead( $job );
	if( defined $job->{when}{start} ) {
		my $d = e2date( time );
		return 0 if ($d cmp $job->{when}{start}) < 0;
	}
	if( $job->{when}{if} ) {
		unless( eval($job->{when}{if}) ) {
			log::debug $job->{fullname}.' dont pass its "if" condition';
			return 0;
		}
		log::debug $job->{fullname}.' validate its "if" condition';
	}
	if( $job->{when}{after} ) {
		my $ok = 0;
		foreach my $or ( split /[|]| or /i, $job->{when}{after} ) {
			my $okand = 1;
			foreach my $and ( split /[&+]| and /i, $or ) {
				$and =~ s/^\s+//;
				$and =~ s/\s+$//;
				$job->{afterdone}{$and} = $jobend->{$and} if $jobend->{$and};
				$okand &= exists $job->{afterdone}{$and};
			}
			$ok |= $okand;
		}
		return $ok;
	}
	1
}

sub dead {
	my $job = shift;
	defined $job->{when}{count} && $job->{when}{count}<1;
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
