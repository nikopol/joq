package log;

use strict;
use warnings;

use DateTime;
use Term::ANSIColor '3.0';

use constant {
	LOGNONE   => 0,
	LOGERROR  => 1,
	LOGWARN   => 2,
	LOGNOTICE => 3,
	LOGINFO   => 4,
	LOGDEBUG  => 5,
	LOGCORE   => 6,
	LOGLEVELS => [ qw/NONE ERROR WARN NOTICE INFO DEBUG CORE/ ],
	LOGCOLORS => [
		'',               #none
		'red',            #error
		'red',            #warning
		'yellow',         #notice
		'',               #info
		'bright_black',   #debug
		'blue',           #core
	],
	PIDCOLORS => [ 
		'bright_magenta', 'green',
		'bright_yellow', 'bright_blue', 'bright_magenta', 
		'bright_green'
	],
	NBPIDCOLORS => 6,
	MODES => [ qw/SHORT LONG COLOR/ ],
	SHORT => 0,
	LONG  => 1,
	COLOR => 2,
};

our $TZ      = $ENV{TZ} || 'Europe/Paris';
my $loglevel = LOGINFO;
my %pids;
my %out; #name=[handle,mode,level,own]
my $rotcount = 0;

#params: logfile or hash options (all optional)
#  file  => filename
#  mode  => short|long|color : color by default if no output defined
#  level => error|warning|info|notice|debug  : set default loglevel

sub setup {
	my %arg = @_;
	$TZ = $arg{timezone} if $arg{timezone};
	level( $arg{level} ) if $arg{level};
	#at least output to stdout
	$arg{console} = delete $arg{mode} || 'color';
	delete $arg{console} if lc($arg{console}) eq 'none';
	addout( '*STDOUT', $arg{console} ) if $arg{console};
	addout( $arg{file}, LONG, $loglevel, $arg{size}||0 ) if $arg{file};
	1;
}

sub config {
	my $c = { level => level() };
	foreach( keys %out ) {
		my($h,$m,$l,$o,$s) = @{$out{$_}};
		if( $_ eq '*STDOUT' ) {
			$c->{console} = MODES->[$m];
		} else {
			$c->{file} = $_;
		}
		$c->{size} = $s if $s;
	}
	$c;
}

sub findlevel {
	my($q,$array,$dft) = @_;
	return $dft unless $q;
	return $q if $q =~ /^[0-9]{1}$/ && $q < @$array;
	my $n=0;
	$n++ while( $n < @$array && $array->[$n] ne uc($q) );
	$n;
}

sub level {
	my $l = shift;
	if( $l ) {
		$loglevel = findlevel($l,LOGLEVELS,LOGINFO) if $l;
		notice('level set to '.LOGLEVELS->[$loglevel]);
	}
	LOGLEVELS->[$loglevel];
}

sub pidcolor {
	my $pid = shift;
    $pids{$pid} = PIDCOLORS->[ (scalar keys %pids) % NBPIDCOLORS ]
        unless exists $pids{$pid};
	$pids{$pid};
}

sub outrotate {
	my $fn = shift;
	my($h,$mode,$lev,$own,$siz) = @{$out{$fn}};
	if( $own && !($fn =~ /^\*[A-Z]+$/) ) {
		close $h;
		my $num = 1;
		$num++ while -r $fn.'.'.$num;
		rename $fn => $fn.'.'.$num;
		open $h, '>>:utf8', $fn;
		$out{$fn} = [ $h, $mode, $lev, $own, $siz ];
	}
}

sub rotate {
	outrotate $_ for keys %out;
	1;
}

sub addout {
	my( $name, $mode, $level, $size, $handle ) = @_;
	return 0 unless $name;
	rmout( $name );
	my $std = $name =~ /^\*[A-Z]+$/;
	my $own = 0;
	unless( $handle ) {
		eval {
			my $fm = $std ? '>&' : '>>:utf8';
			open $handle, $fm, $name;
			$own = 1;
		};
		warning('unable to open '.$name.' : '.$@) if $@;
	}
	return 0 unless $handle;
	my $l = defined $level ? findlevel($level,LOGLEVELS,LOGINFO) : $loglevel;
	my $m = findlevel($mode,MODES,COLOR);
	my $s = 0;
	if( !$std && $size ) {
		my($n,$u) = $size =~ m/(\d+)([okmg])/i;
		my %cf = ( k=>1000, m=>1000000, g=>1000000000 );
		if( $n ) {
			$s = $n;
			$u = lc $u;
			$s *= $cf{$u} if $cf{$u};
		}
	}
	$out{$name} = [ $handle, $m, $l, $own, $s ];
	debug($name.' opened (level='.($l?LOGLEVELS->[$l]:'default').',mode='.MODES->[$m].($s?",size=$s":'').')');
	$name;
}

sub rmout {
	my $name = shift;
	return 0 unless exists $out{$name};
	eval { close $out{$name}->[0] if $out{$name}->[3]; };
	delete $out{$name};
	debug($name.' closed');
	1;
}

#func(msg[,from[,pid]])
sub core    { LOG( shift, shift || (caller())[0], LOGCORE, shift); }
sub debug   { LOG( shift, shift || (caller())[0], LOGDEBUG, shift); }
sub error   { LOG( shift, shift || (caller())[0], LOGERROR, shift); }
sub warn    { LOG( shift, shift || (caller())[0], LOGWARN, shift); }
sub warning { LOG( shift, shift || (caller())[0], LOGWARN, shift); }
sub info    { LOG( shift, shift || (caller())[0], LOGINFO, shift); }
sub notice  { LOG( shift, shift || (caller())[0], LOGNOTICE, shift); }

sub LOG {
    my( $msg, $from, $level, $pid ) = @_;
	my @outkeys = keys %out;
	unless( @outkeys ) {
		#show error even if no log output defined
		CORE::warn "$msg\n" if $level == LOGERROR;
		return 1;
	}
 	$from =~ s/^.*\://g;
	$from = uc $from;
	$from .= "#$pid" if $pid;
	$level ||= LOGINFO;
	my $d = DateTime->now( time_zone=>$TZ );
	my $c1 = LOGCOLORS->[$level];
	my $c2 = $pid ? pidcolor( $pid ) : $c1;
	my @fmt = (
		#SHORT
		sprintf( "%s|%-6s|%12s|%s\n", $d->hms, LOGLEVELS->[$level], $from, $msg ),
		#LONG
		sprintf( "%s %s|%-6s|%12s|%s\n", $d->ymd, $d->hms, LOGLEVELS->[$level], $from, $msg),
		#COLOR
		$d->hms.'|'.
		colored(sprintf('%-6s',LOGLEVELS->[$level]),$c1).'|'.
		colored(sprintf('%12s',$from),$c2).'|'.
		colored($msg,$c2)."\n",
	);
	foreach my $fn ( @outkeys ) {
		my($h,$m,$l,$o,$s) = @{$out{$fn}};
		$l = $loglevel unless defined $l;
		if( $level <= $l ) {
			syswrite $h, $fmt[$m];
			outrotate $fn if $s && -s $fn >= $s;
		}
	}
	$@ = $msg if $level == LOGERROR;
	1;
}

1;
__END__

=head1 NAME

LOG

=head1 SYNOPSIS

  use LOG;

  log::setup(stdout=>'color',loglevel=>'debug');
  log::info('hello world');

=head1 DESCRIPTION

=head1 AUTHOR

E<lt>nikomomo@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
