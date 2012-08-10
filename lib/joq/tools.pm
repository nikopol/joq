package joq::tools;

use warnings;
use strict;
use 5.010;
use Encode;
use base 'Exporter';

our @EXPORT = qw(
	hmerge
	deepcopy
);

sub hmerge {
	my( $a, $b ) = @_;
	for my $k ( keys %$b ) {
		$a->{$k} = ( !exists $a->{$k} || ref($a->{$k}) ne 'HASH' ) 
			? $b->{$k}
			: hmerge( $a->{$k}, $b->{$k} );
	}
	$a;
}

sub deepcopy {
	my( $h, $utf8 ) = @_;
	return( $utf8 ? decode_utf8($h) : $h) unless ref($h);
	if( ref($h) eq 'HASH' ) {
		my $c = {};
		$c->{$_} = deepcopy($h->{$_},$utf8) for keys %$h;
		return $c;
	}
	if( ref($h) eq 'ARRAY' ) {
		my $c = [];
		push @$c, deepcopy($_,$utf8) for @$h;
		return $c;
	}
	undef;
}

1