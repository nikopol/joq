package joq;

use warnings;
use strict;
use 5.010;

use POSIX ':signal_h';
use Socket;
use AnyEvent;
use AnyEvent::Socket;
use Time::HiRes qw( sleep );
use IO::Socket;

use joq::file;
use joq::logger;
use joq::queue;
use joq::job;
use joq::remote;
use joq::output;
use joq::tools;

use constant {
	SHELLCLOSE  => 0,
	SHELLOK     => 1,
	SHELLOKNP   => 2,
};

our $VERSION = '0.0.23';

our %cfg = (
	server    => 'localhost:1970',
	oneshot   => 0,
	backup    => 0,
	polling   => 15,
);

my $started  = 0;
my $softstop = 0;
my %watch;
my $w;

sub init {
	my %arg = @_;
	my $file = delete $arg{load};
	my $path;
	if( $file ) {
		$path = $1 if $file =~ m|^(.*/)[^/]+$|;
		%arg = ( %{load($file)||{}}, %arg );
	}
	#setup log
	$arg{log} = {} unless exists $arg{log};
	for(qw(level file console size)) {
		$arg{log}->{$_} = delete $arg{"log_$_"} if exists $arg{"log_$_"};
	}
	log::setup( %{$arg{log}} );
	log::notice('JoQ version='.$VERSION.' pid='.$$.' uid='.$<);
	#setup core/queue/job
	for(keys %joq::cfg)        { joq::config( $_, $arg{$_} ) if exists $arg{$_} }
	for(keys %joq::queue::cfg) { joq::queue::config( $_, $arg{$_} ) if exists $arg{$_} }
	for(keys %joq::job::cfg)   { joq::job::config( $_, $arg{$_} ) if exists $arg{$_} }
	#setup remotes
	if( $arg{remotes} ) {
		my @l = ref($arg{remotes}) eq 'ARRAY' ? @{$arg{remotes}} : split /,/,$arg{remotes};
		joq::remote::add( $_ ) foreach( @l );
	}
	#enqueue jobs
	addjobs( $arg{jobs}, $path ) if $arg{jobs};
}

sub load {
	my( $file, $path ) = @_;
	my $data = {};
	$file = $path.$file if $path && $file !~ m|^/| && -f $path.$file;
	if( $file ) {
		my $f = length($file)>768 ? substr($file,0,768)."... [".length($file)." bytes]" : $file;
		eval {
			log::info("read/parse $f");
			$data = parsefile( $file );
		};
		log::error("read/parse $f : $@") if $@;
	}
	$data;
}

sub loadjobs {
	my( $list, $path ) = @_;
	$list = [ $list ] unless ref($list) eq 'ARRAY';
	$path = $path unless defined $path;
	my @jobs;
	log::info('loading '.scalar @$list.' jobs');
	for my $j ( @$list ) {
		if( ref($j) eq 'HASH' ) {
			if( exists $j->{extend} ) {
				my( $file, $name ) = split /:/, delete $j->{extend};
				my $data = load( $file, $path );
				if( ref($data) eq 'HASH' && $data->{jobs} ) {
					my @exjobs = grep { !$name || $name eq $_->{name} } @{$data->{jobs}};
					my $ex = shift @exjobs;
					if( $ex ) {
						log::debug('extend '.($j->{name}||$ex->{name}||'').' with '.$file.':'.($name||'first'));
						$j = hmerge( $ex, $j );
					} else {
						log::error("extend from $file, job ".($name||"first")." not found");
						next;
					}
				} else {
					log::error("extend from bad $file, ignored");
					next;
				}
			}
			push @jobs, $j;
		} elsif( ref($j) eq '' ) {
			my $data = load( $j, $path );
			push @jobs, @{loadjobs( $data->{jobs}, $path )}
				if $data && ref($data) eq 'HASH' && exists $data->{jobs};
		} else {
			log::error('wrong job definition '.ref($j).' ignored');
		}
	}
	\@jobs;
}

sub backup {
	$cfg{backup} ? save( $cfg{backup} ) : 0;
}

sub save {
	my $fn = shift;
	eval {
		my $s = {
			%cfg,
			%joq::queue::cfg,
			%joq::job::cfg,
			log     => log::config(),
			jobs    => [ 
				map {
					my $job = deepcopy $_, 1;
					for(qw[args fixeday lastout lasterr]){
						delete $job->{$_} unless ref($job->{$_}) ne 'ARRAY' || @{$job->{$_}};
					}
					delete $job->{$_} for qw(afterdone fixeday id laststart pid fullname order lastimeout);
					delete $job->{when}{$_} for qw(start ntime ndayofweek ndayofmonth ndayofyear);
					$job
				} joq::queue::jobs 
			],
			remotes => [ joq::remote::find('all') ],
		};
		delete $s->{$_} for qw(backup);
		writefile( $fn, $s, 'yaml' );
		log::debug('state saved in '.$fn);
	};
	if( $@ ) {
		log::error('error saving '.$fn.' : '.$@);
		undef $fn;
	}
	$fn;
}

sub addjobs { joq::queue::addjobs( loadjobs( @_ ) ) }
sub addjob  { joq::queue::addjob( @_ ) }

sub config {
	my( $key, $val ) = @_;
	return \%cfg unless $key;
	return undef unless exists $cfg{$key};
	if( defined $val ) {
		if( $key eq 'polling' && ( 0+$val != $cfg{polling} || !$watch{poll} ) ) {
			$val = 1 if 0+$val < 1;
			$cfg{polling} = 0+$val;
			setpoll();
		} else {
			$cfg{$key} = $val;
			log::info("$key set to $val");
		}
	}
	$cfg{$key};
}

sub stopevents {
	# for( keys %watch ){
	# 	my $w = delete $watch{$_};
	# 	$w->destroy;
	# }
	delete $watch{$_} for keys %watch;
}

sub setpoll {
	my $sec = shift || $cfg{polling};
	delete $watch{poll} if $watch{poll};
	$watch{poll} = AnyEvent->timer(
		after    => 0,
		interval => $sec,
		cb       => sub {
			if( (time - $joq::queue::pollend) > 0 ){
				joq::poll('polling');
			} else {
				log::core('poll skipt');
			}
		},
	);
	log::info('polling set to '.$sec.'s');
}

sub poll {
	my( $queued, $running, $event ) = joq::queue::poll( $softstop, shift );
	#log::core("queue polled return queued=$queued running=$running event=$event");
	$w->send('oneshot') if !$queued && $cfg{oneshot};
	$w->send('soft stop') if !$running && $softstop;
}

sub run {
	return -1 if $started;
	$started = 1;
	init( @_ );

	my $start = time;
	$w = AnyEvent->condvar;

	$watch{sigint} = AnyEvent->signal(
		signal => 'INT',
		cb     => sub {
			if( $softstop ) {
				log::warn('SIGINT! hard stop');
				$w->send( 'hard stop' );
			} else {
				log::warn('SIGINT! soft stop - slap me again to hard stop');
				$softstop = 1;
				joq::poll('quit');
			}
		},
	);

	$watch{sigusr1} = AnyEvent->signal(
		signal  => 'USR1',
		cb      => sub {
			log::core('SIGUSR1! log rotate');
			log::rotate;
		}
	);

	$watch{sigusr2} = AnyEvent->signal(
		signal  => 'USR2',
		cb      => sub {
			log::core('SIGUSR2! polling');
			joq::poll('signal');
		}
	);

	$watch{child} = AnyEvent->child(
		pid => 0, #$pid, 
		cb  => sub {
			my( $pid, $status ) = @_;
			if( my $job = joq::queue::jobbypid( $pid ) ) {
				log::core('SIGCHLD! '.$pid.' return '.$status);
				joq::job::ending( $job => joq::job::ENDCHILD, $status );
			} else {
				log::core('SIGCHLD! '.$pid.' not found?! return '.$status);
			}
		}
	);

	my %tcp_commands;
 	if( $cfg{server} && $cfg{server} !~ /^(?:off|false|disabled?)$/i ) {

		%tcp_commands = (

			add => { alias => 'addjob' },
			addjob => {
				txt => <<EOTXT
add a job in queue
    options:   name=jobname  : set job's nickname (must be unique)
              delay=seconds  : time to wait from job creation before start
             repeat=seconds  : time between runs (next_run=last_start+repeat)
              count=count    : how many times you wanna run this job 
			                   (default=1 or infinite if "repeat" used)
                 if=codeval  : not startable unless this codeval
			  alone=1        : setup alone flag
               nice=-20..19  : nice job's fork (-20=fast,19=slow)
           priority=1..10    : start priority (1=low,10=high,default=5)	 
              after=job(s)   : wait then end of this/theses job(s) to start.
                               you can use boolean & and | between jobnames.
                               eg: jobname eg: job1&job2 eg: job1|job2
          dayofweek=day(s)   : run job at theses days, comma separeted. 
                               avail values: all,sunday,dimanche,monday,lundi,etc...
         dayofmonth=day(s)   : run job at theses days, comma separated (all, 1-31)
          dayofyear=day(s)   : run job at theses days, comma separated (all, 1-365)
               time=time(s)  : run job at theses hours. eg: 1h00,15h00
            logfile=filename : log filename of job output

   eg: addjob ping rtgi.eu repeat=1m count=10
EOTXT
				,
				arg => "[shell|code|class] cmd [args] [opts]",
				bin => sub {
					my( $out, $arg ) = @_;
					my @args = split /\s/,$arg;
					my $cmd = shift @args;
					my $typ = 'shell';
					if( $cmd && $cmd =~ /^(shell|code|class)$/i ) {
						$typ = lc $cmd;
						$cmd = shift @args;
					}
					if( $cmd ) {
						my %jobargs = (
							$typ  => $cmd,
							when  => {},
							args  => [],
						);
						foreach my $a ( @args ) {
							my($k,$v) = $a =~ /^([^=]+)=(.+)$/;
							if( $k && $k =~ /^(?:name|logfile|nice|priority|timeout)$/i && defined $v ) {
								$jobargs{$k} = $v;
							} elsif( $k && $k =~ /^(?:delay|repeat|count|after|dayofweek|dow|dayofmonth|dom|dayofyear|doy|time|if|alone)$/i && defined $v ) {
								log::debug("set when $k = $v");
								$jobargs{when}->{$k} = $v;
							} else {
								$jobargs{$typ}.= ' '.$a;
							}
						}
						unless( $jobargs{name} ) {
							my $name = $cmd;
							$name =~ s/[^\w]+.*$//g;
							if( joq::queue::job($name) ) {
								my $n = 2;
								$n++ while( joq::queue::jobbyname($name.'#'.$n) );
								$name = $name.'#'.$n;
							}
							$jobargs{name} = $name;
						}
						my $job = addjob( \%jobargs );
						if( $job ) {
							$out->send($job->{fullname}.' queued');
							kill SIGUSR2 => $$; #poll
							backup;
						} else {
							$out->error($@);
						}
					} else {
						$out->error('specify a script or a command');
					}
					SHELLOK;
				}
			},

			close => {
				txt => "close this connection",
				bin => sub { SHELLCLOSE }
			},
			
			del => { alias => 'deljob' },
			deljob => {
				txt => "remove a job from the queue, stopping it if needed",
				arg => "jobid|jobname",
				bin => sub {
					my( $out, $arg ) = @_;
					my @lines;
					if( $arg ) {
						foreach( split /\s+/,$arg ) {
							if( joq::queue::deljob( $_ ) ) {
								push @lines, 'job '.$_.' removed';
								backup;
							} else {
								push @lines, 'job '.$_.' not found';
							}
						}
						$out->dump(\@lines,'deljob');
					} else {
						$out->error('what job ?');
					}
					SHELLOK;
				}
			},

			exit => { alias => 'close' },
			halt => { alias => 'shutdown' },

			help => {
				txt => "order a pepperoni pizza",
				arg => "[cmd1 [cmd2 ...]]",
				bin => sub {
					my( $out, $arg ) = @_;
					my @args = $arg ? split /\s+/,$arg : ();
					my @cmds = sort keys %tcp_commands;
					if( @args ) {	
						my %help;
						foreach my $cmd ( @args ) {
							if( my $c = $tcp_commands{$cmd} ) {
								$c = $tcp_commands{$c->{alias}} if $c->{alias};
								my @alias = grep { $tcp_commands{$_}->{alias} && $tcp_commands{$_}->{alias} eq $cmd } @cmds;
								my %o;
								$o{args}    = $c->{arg} if exists $c->{arg};
								$o{alias}   = join(', ',@alias) if @alias;
								$o{purpose} = [split(/\n/,$c->{txt})] if $c->{txt};
								$help{$cmd} = \%o;
							} else {
								$help{$cmd} = 'not found';
							}
						}
						$out->dump(\%help);
					} else {
						my @lines;
						foreach( @cmds ) {
							my $c = $tcp_commands{$_};
							my @txts = $c->{txt} ? split(/\n/,$c->{txt}) : ();
							push @lines, {
								cmd  => $_,
								args => $c->{arg}||'',
								help => $c->{alias} ? 'alias of '.$c->{alias} : @txts ? shift @txts : '',
							};
						}
						$out->dump(\@lines,'help','cmd,args,help');
					}
					SHELLOK;
				}
			},

			history => {
				txt => 'show details on a dead job or all lasts dead jobs',
				arg => '[jobid|jobname]',
				bin => sub {
					my( $out, $arg ) = @_;
					my @jobs = joq::queue::deadjobs( $arg );
					if( @jobs ) {
						my @lines;
						foreach my $job ( @jobs ) {
							push @lines, {
								id        => $job->{id},
								name      => $job->{name},
								laststart => $job->{laststart},
								lastend   => $job->{lastend},
								duration  => joq::job::duration($job),
								exitcode  => $job->{exitcode},
								lastout   => $job->{lastout},
								lasterr   => $job->{lasterr}

							};
						}
						$out->dump(\@lines,'history');
					} else {
						$out->error('jobs not found');
					}
					SHELLOK;
				}
			},

			kill => { alias => 'deljob' },

			killall => {
				txt => "stop all running job and flush queue",
				bin => sub {
					my $out = shift;
					$out->send(joq::queue::killall().' job(s) flushed');
					SHELLOK;
				}
			},

			list => {
				txt => "list jobs in queue",
				bin => sub {
					my($out,$arg) = @_;
					my @jobids = joq::queue::jobids;
					if( @jobids ) {
						my @l = $arg ? split /\s+/,$arg : @jobids;
						my @lines;
						foreach( @l ) {
							my $job = joq::queue::job( $_ );
							next unless $job;
							my $j = {
								id     => $job->{id},
								name   => $job->{name},
								status => joq::queue::running($_) ? 'running pid='.$job->{pid} : 'pending',
							};
							for my $k (qw/start count after run repeat runcount exitcode/) {
								my $v = defined $job->{when}{$k}
									? $job->{when}{$k} 
									: defined $job->{$k} 
										? $job->{$k}
										: undef;
								if( $k eq 'after' && $v ) {
									$v =~ s/$_/[$_]/ foreach( keys %{$job->{afterdone}} );
								}
								$j->{$k} = $v if defined $v;
							}
							push @lines, $j;
						}
						my $z=\@lines;
						$out->dump($z,'list','id,name,alias,args');
					} else {
						$out->send('empty queue');
					}
					SHELLOK;
				}
			},

			load => {
				txt => <<EOTXT
load jobs from a file or string.
yaml and json are both accepted.
only the "jobs" key will be processed, other entries such as joq parameters are ignored.
EOTXT
				,
				arg => 'filename or string',
				bin => sub {
					my($out,$arg) = @_;
					my $path = $1 if -f $arg && $arg =~ m|^(.*/)[^/]+$|;
					my $data = load( $arg );
					if( $data && ref($data) eq 'HASH' ) {
						my $jobs = exists $data->{jobs} ? $data->{jobs} : $data;
						my $adds = addjobs( $jobs, $path  );
						if( @$adds ) {
							$out->dump($adds,'adds');
							backup();
							poll();
						} else {
							$out->send('no job queued');
						}
					} else {
						$out->send('file '.$arg.' not found');
					}
					SHELLOK;
				}
			},

			log => {
				txt => 'show realtime log. press enter to stop.',
				arg => '[short|long|color] [level]',
				bin => sub { 
					my($out,$arg,$cnxid) = @_;
					my($mod,$lev);
					foreach( split /\s+/,$arg ) {
						$lev = $_ if /error|warning|info|notice|debug/i;
						$mod = $_ if /short|long|color/i;
					}
					log::addout($cnxid,$mod,$lev,0,$out->{fh});
					SHELLOKNP;
				}
			},
			
			mode => {
				txt => 'set server output format',
				arg => '[text|json|yaml]',
				bin => sub {
					my($out,$arg) = @_;
					$out->send('mode set to '.$out->setmode($arg));
					SHELLOK;
				}
			},

			pause => {
				txt => 'set queue in pause',
				bin => sub {
					shift->send( joq::queue::pause()
						? 'queue paused'
						: 'queue already paused'
					)
				}
			},

			quit  => { alias => 'close' },

			rm => { alias => 'del' },

			remote => {
				txt => 'show/add/del a remote joq',
				arg => '[add name host:port [sync]|del name]',
				bin => sub {
					my($out,$arg) = @_;
					if( $arg ) {
						my @args = split /\s+/, $arg;
						my $cmd = shift @args;
						if( $cmd =~ /del|\-/i ) {
							$out->send(joq::remote::del( shift @args ).' remotes deleted');
						} elsif( $cmd =~ /add|\+/i ) {
							if(joq::remote::add( join ' ',@args )) {
								$out->send('remote added');
							} else {
								$out->error('host:port wanted');
							}
						} else {
							$out->error('add or del ?');
						}
					} elsif( %joq::remote::joqs ) {
						my @lines;
						foreach( keys %joq::remote::joqs ) {
							my $s = $joq::remote::joqs{$_};
							push @lines, $s->{name}.' '.$_.' '.($s->{sync}?"sync'ed":'');
						}
						$out->dump(\@lines,'remote');
					} else {
						S$out->error('no remote server');
					}
					SHELLOK;
				}
			},

			resume => {
				txt => 'resume queue',
				bin => sub {
					shift->send( joq::queue::resume()
						? 'queue resumed'
						: 'queue not paused'
					)
				}
			},
		
			save => {
				txt => 'save current joq state to a file',
				arg => 'filename',
				bin => sub {
					my($out,$arg) = @_;
					if( $arg ) {
						if(save($arg)) {
							$out->send($arg.' saved');
						} else {
							$out->error('saving '.$arg);
						}
					} else {
						$out->error('what file?');
					}
					SHELLOK;
				}
			},

			set => { alias => 'setup' },
			setup => {
				txt => 'set (or get without value) an joq parameter',
				txt => <<EOTXT
set (or get without value) an joq parameter
 parameters:  polling [sec] : delay between two jobs polling
              oneshot [0|1] : halt when queue is empty
              backup [file] : save joq state
               maxfork [nb] : maximum simultaneous jobs
            maxhistory [nb] : max job kept in the history
          termtimeout [sec] : delay between a TERMinate and a KILL
                              when you stop a running job
             loglevel level : error|warning|notice|info|debug
EOTXT
				,
				arg => 'parameter [value]',
				bin => sub {
					my($out,$arg) = @_;
					my $params = join('|', keys %joq::cfg, keys %joq::queue::cfg, keys %joq::job::cfg );
					my @args   = split /\s+|\=/, ($arg || '');
					my $key    = shift @args;
					unless( $key || ! $key =~ /^$params$/ ) {
						$out->error('what parameter ?');
					} elsif( exists $joq::cfg{$key} ) {
						$out->send($key.' set to '.joq::config($key, shift @args));
					} elsif( exists $joq::queue::cfg{$key} ) {
						$out->send($key.' set to '.joq::queue::config($key, shift @args));
					} elsif( exists $joq::job::cfg{$key} ) {
						$out->send($key.' set to '.joq::job::config($key, shift @args));
					} elsif( $key eq 'loglevel' ) {
						$out->send($key.' set to '.log::level(shift @args));
					}
					SHELLOK;
				}
			},

			show => {
				txt => 'show job\'s bowls',
				arg => 'jobid|jobname|all|run',
				bin => sub {
					my($out,$arg) = @_;
					if( $arg ) {
						if( $arg eq 'all' ) {
							my @jobs = map { delete $_->{logfh}; $_ } ( joq::queue::jobs() );
							$out->dump(\@jobs,'jobs');
						} elsif( $arg eq 'run' ) {
							my @jobs = map { delete $_->{logfh}; $_ } ( joq::queue::runjobs() );
							$out->dump(\@jobs,'running');
						} else {
							my $job = joq::queue::job( $arg );
							if( $job ) {
								$job = { %$job };
								delete $job->{logfh};
								$out->dump($job,'job');
							} elsif( my $deads = [ joq::queue::deadjobs( $arg ) ] ) {
								$out->dump($deads, 'history') if $deads;
							} else {
								$out->error('job not found');
							}
						}
					} else {
						$out->error('what job?');
					}
					SHELLOK;
				}
			},

			shutdown => {
				txt => "send the daemon to the graveyard",
				bin => sub {
					unless( $softstop ) {
						shift->send('starting soft stop ('.joq::queue::runcount().' jobs running)');
						$softstop = 1;
						return SHELLOK;
					}
					$w->send( 'stopped from server' );
					SHELLCLOSE;
				}
			},

			stat   => { alias => 'status' },
			status => {
				txt => "show JoQ various states",
				bin => sub {
					my $out = shift;
					my $s = time - $start;
					my $m = int($s / 60);
					my $h = int($m / 60);
					my $d = int($h / 24);
					my $stat = joq::queue::status();
					$stat->{uptime} = sprintf("%d day%s %dh%02dm%02ds", $d, $d>1?'s':'', $h%24, $m%60, $s%60);
					$out->dump($stat,'status');
					SHELLOK;
				}
			},

			stop => { alias => 'stopjob' },
			stopjob => {
				txt => "stop a running job and let it queued",
				arg => "jobid|jobname",
				bin => sub {
					my($out,$arg) = @_;
					if( $arg ) {
						$out->send(
							joq::queue::stopjob( $arg )
								? 'job stopped'
								: 'job not found or not running'
						);
					} else {
						$out->error('what job?');
					}
					SHELLOK;
				}
			},

			sync => {
				txt => "sync a remote joq",
				arg => "all|name",
				bin => sub {
					shift->send(joq::remote::sync(shift).' servers synced');
					SHELLOK;
				}
			},

			unsync => { 
				txt => "unsync a remote joq",
				arg => "all|name",
				bin => sub {
					shift->send(joq::remote::unsync(shift).' servers unsynced');
					SHELLOK;
				}
			},

		);
		
		my( $ip, $port ) = parse_hostport($cfg{server});
		if( $ip ) {
			$ip = Socket::inet_aton( $ip );
			$ip = format_address( $ip ) if $ip;
		}
		$port = 1970 unless $port;
		unless( $ip ) {
			log::error('invalid server host (ip not found), switch to localhost:'.$port);
			$ip   = '127.0.0.1';
			$port = 1970;
		};
		my $sock = IO::Socket::INET->new(PeerAddr=>$ip,PeerPort=>$port,Proto=>'tcp');
		if( $sock ) {
			close $sock;
			stopevents;
			log::error("joq already present on $ip:$port");
			die "\n";
		}
		tcp_server( $ip, $port, sub {
			my($fh, $host, $port) = @_;
			my $cnxid = "$host:$port";
			log::debug("connection opened from $cnxid");
			syswrite $fh, <<EOINTRO

 ██████████   zogzog to joq v$VERSION
  █ █ █ █ █   please let this space as clean as when you logged in
███ ███ ████  try help if you're lost - empty line repeat last command

EOINTRO
			;
			syswrite $fh, '>';
			my $out = joq::output->new;
			my $lastcmd = 'status';
			my $buf = '';
			my $buflen = 0;

			my $io; $io = AnyEvent->io(
				fh   => $fh,
				poll => 'r',
				cb   => sub {
					log::rmout( $cnxid );
					my $rcv= <$fh>;
					unless( $rcv ) {
						undef $io;
					} else {

						$buf .= $rcv;	
						if( !$buflen && $buf =~ /^<(\d+)>/ ) {
							$buflen = $1;
							$buf =~ s/^<\d+>//;
						}

						my $cr = SHELLOK;
						if( $buf && (($buflen && length($buf)>=$buflen) || (!$buflen && $buf =~ /\n$/)) ) {
							chomp $buf;
							$out->{fh} = $fh;
							my @lines = $buflen ? ( $buf ) : split /\n/, $buf;
							foreach my $line ( @lines ) {
								$line =~ s/^\s+//;
								$line =~ s/\s+$//;
								if( $line ) {
									$lastcmd = $line if $line =~ /^(stat|list|history)/i;
								} else {
									$line = $lastcmd;
								}
								$line =~ s/^(\w+)(\s|$)//;
								my $cmd = $1 || '';
								$line =~ s/^\s+//;
								my @at;
								my $arg;
								if( $line =~ /^\{.*\}$/ ) {
									$arg = $line;
								} else {
									my @args = split / /, $line;
									#check for @host directive
									@at = grep { /^@/ } @args;
									@args = grep { ! /^@/ } @args;
									$arg = join ' ', @args;
								}
								if( my $sub = $tcp_commands{$cmd} ) {
									if( $sub->{alias} ) {
										$cmd = $sub->{alias};
										$sub = $tcp_commands{$cmd};
									}
									my @remotes;
									if( @at ) {
										map { s/^@// } @at;
										foreach( @at ) {
											my @atok = joq::remote::find($_);
											if( @atok ) { push @remotes, @atok; }
											else { syswrite $fh, "unknow server $_\n"; }
										}
										my %h;
										@remotes = grep { !$h{$_}++ } @remotes;
									} else {
										@remotes = joq::remote::synced;
									}
									my $dbg = "$cmd($arg)";
									$dbg = substr($dbg,0,253).'...' if length($dbg)>256;
									log::debug("execute $dbg [".length($arg)." bytes] from $cnxid");
									joq::remote::exec("$cmd $arg", \@remotes, $fh) 
										if @remotes && $cmd =~ /load|list|show|add|del|stop|killall|shutdown/;
									$cr = $sub->{bin}($out,$arg,$cnxid) unless @at;
									undef $io if $cr == SHELLCLOSE;
								} else {
									syswrite $fh, "what?! try \"help\"\n";
								}
							}
							syswrite $fh, '>' if $cr == SHELLOK;
							$buf = '';
							$buflen = 0;
						 }
					}
					log::debug("connection closed from $cnxid") unless $io;
				},
			);
		});
		log::info('telnet server started on '.$ip.':'.$port);
	}

	log::core('event loop, impl='.AnyEvent::detect);
	backup;
	log::notice('JoQ started');
	setpoll() unless $watch{poll};
	my $r = $w->recv;
	backup;
	joq::queue::killall;
	log::notice('shutdown ('.$r.')');
	$r;
}

1;
__END__

=head1 NAME

joq 

=head1 SYNOPSIS

  use joq;
  joq::load(shift @ARGV) if @ARGV;
  joq::run;

=head1 DESCRIPTION

joq is a JObs Queue

=head1 AUTHOR

niko E<lt>nikomomo@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
