package joq::file;

use strict;
use warnings;

use YAML::XS;
use JSON::XS;

use base 'Exporter';
our @EXPORT = qw(readfile writefile parsefile);

use constant BUFSIZE => 16384;

sub readfile {
	my $fn   = shift || die('what file?');
	my %args = @_ || ();
	my $mode = $args{mode} ? $args{mode} : $args{no_utf8} ? '<' : '<:encoding(UTF-8)';
	open(FH, $mode, $fn) or die('cannot open file '.$fn.' : '.$!);
	my $data = '';
	read( FH, $data, BUFSIZE, length $data ) while !eof(FH);
	close FH;
	wantarray ? split /\r\n|\n/, $data : $data;
}

sub parsefile {
	my $fn  = shift || die('what file?');
	my $buf = shift || ( -f $fn ) ? readfile( $fn ) : $fn;
	( $buf =~ /^\s*\{/ ) ? decode_json( $buf ) : Load( $buf );
}

sub writefile {
	my $fn   = shift || die('what file?');
	my $data = shift || '';
	my $fmt  = shift;
	if( ref($data) eq 'ARRAY' ) {
		$data = join("\n", @$data);
	} elsif( ref($data) eq 'HASH' ) {
		unless( $fmt ) { $fmt = ( $fn =~ /\.ya?ml$/i ) ? 'yaml' : 'json'; }
		$data = ( $fmt eq 'yaml' ) ? Dump( $data ) : encode_json( $data );
	}
	my %args = ( @_ );
	my $mode = $args{mode} ? $args{mode} : $args{append} ? '>>' : '>';
	open(FH, $mode, $fn) or die('cannot open file '.$fn.' : '.$!);
	print FH $data;
	close FH;
	length $data;
}

1;

