package joq::output;

use JSON::XS;
use YAML::XS;

use joq::tools;

sub new {
	my $class = shift;
	my $self = {
		h      => undef,
		mode   => 'text',
		dumper => undef,
	};
	bless $self, $class;
}

sub setmode {
	my($self,$mode) = @_;
	return $self->{mode} if !$mode || $mode eq $self->{mode} || $mode !~ /^(?:json|text|yaml)$/;
	$self->{dumper} = $mode eq 'json' ? JSON::XS->new->utf8->pretty : undef;
	$self->{mode} = $mode;
}

sub textmode { shift->{$mode} eq 'text' }
sub send { shift->dump(shift, 'msg') }
sub error { shift->dump(shift,'error') }
sub lf { syswrite shift->{fh}, "\n"; }

sub dump {
	my($self,$txt,$key,$order) = @_;
	my $out = '';
	if($self->{mode} eq 'text') {
		$key = undef if $key && $key ne 'error';
		if( ref($txt) eq 'HASH' ) {
			$out = $self->list($key,$txt,'',$order);
		} elsif( ref($txt) eq 'ARRAY' ) {
			my $sref = scalar @$txt ? ref(@$txt[0]) : undef;
			if( $sref eq 'HASH' ) {
				$out = $self->table($txt,$order);
			} elsif( $sref eq 'ARRAY' ) {
				$out = $self->table($txt,$order);
			} else {
				$out = join("\n",@$txt);
			}
		} else {
			$out = $key ? $key.': '.$txt : $txt;
		}
	} elsif($self->{dumper}) {
		$out = $self->{dumper}->encode($key?{$key=>$txt}:$txt);
	} else {
		$out = $key ? Dump({$key => $txt}) : Dump($txt);
	}
	syswrite $self->{fh}, $out;
	syswrite $self->{fh}, "\n" if $self->{mode} eq 'text';
}

sub center {
	my($self,$txt,$w) = @_;
	my $len = length($txt);
	if( $len < $w ) {
		my $w1 = int(($w-$len)/2);
		my $w2 = $w-$w1-$len;
		$txt = (' ' x $w1).$txt.(' ' x $w2);
	} elsif( $len > $w ) {
		$txt = substr($txt,0,$w);
	}
	$txt;
}

sub left {
	my($self,$txt,$w) = @_;
	my $len = length($txt);
	if( $len < $w ) {
		$txt = $txt.(' ' x ($w-$len));
	} elsif( $len > $w ) {
		$txt = substr($txt,0,$w);
	}
	$txt;
}

sub right {
	my($self,$txt,$w) = @_;
	my $len = length($txt);
	if( $len < $w ) {
		$txt = (' ' x ($w-$len)).$txt;
	} elsif( $len > $w ) {
		$txt = substr($txt,-$w);
	}
	$txt;
}

sub orderkeys {
	my($self,$keys,$order) = @_;
	my %w;
	if( $order ) {
		my $n = 1;
		foreach( split /[,;:|]+/,$order ) {
			$w{$_} = $n++ if $_;
		}
	}
	sort { ($w{$a}||99) <=> ($w{$b}||99) } @$keys;
}

my %fmtab = (
	lastend   => sub { joq::job::e2date(shift) },
	laststart => sub { joq::job::e2date(shift) },
	repeat    => sub { shift },
	delay     => sub { shift },
);

sub fmt {
	my($self,$key,$val) = @_;
	return $val unless $key;
	if( ref($val) eq 'ARRAY') {
		join(',',@$val);
	} elsif( ref($val) eq 'HASH' ) {
		my @buf;
		push @buf,$_.'='.$val->{$_} foreach( keys %$val );
		join(',',@buf);
	} elsif( my $sub = $fmtab{$key} ) {
		&$sub($val);
	} else {
		$val;
	}
}

sub list {
	my($self,$key,$list,$tab,$order) = @_;
	$tab = '' unless defined $tab;
	$list = '' unless defined $list;
	my $out = '';
	if( ref($list) eq 'HASH' ) {
		my @keys = keys %$list;
		@keys = $self->orderkeys( \@keys, $order );
		my $klen = 0;
		foreach( @keys ) {
			$klen = length($_) if length($_) > $klen;
		}
		my $stab = $tab.($klen>5 ? ' ' x int($klen/2) : '   ').' |';
		$out = "\n" if length($tab);
		foreach my $k ( @keys ) {
			$out.=$tab.$self->right($k,$klen).': '.$self->list($k,$list->{$k},$stab,$order);
		}
	} elsif( ref($list) eq 'ARRAY' ) {
		$out = "\n" if length($tab);
		foreach( @$list ) {
			$out .= $tab.$self->fmt($key,$_)."\n";
		}
	} else {
		$out = $self->fmt($key,$list)."\n";
	}
	$out;
}

sub table {
	my($self,$obj,$order) = @_;
	#calc width/columns
	my %cols;
	my @colkeys;
	my $hmode = scalar @$obj && ref($obj->[0]) eq 'HASH';
	my $list = deepcopy $obj;
	if( $hmode ) {
		for my $row (@$list) {
			for $cell ( keys %$row )  {
				$row->{$cell} = $self->fmt($cell,$row->{$cell});
				my $l = length( $row->{$cell} );
				$l = length($cell) if length($cell) > $l;
				$cols{$cell} = $l unless $cols{$cell} && $l < $cols{$cell};
			}
		}
		@colkeys = keys %cols;
		@colkeys = $self->orderkeys( \@colkeys, $order );
	} else {
		for my $row (@$list) {
			my $n = 0;
			for $cell ( @$row )  {
				my $l = length( $cell );
				$cols{$n} = $l unless $cols{$n} && $l < $cols{$n};
				$n++;
			}
		}
		@colkeys = keys %cols;
	}
	#calc utf8 line separator
	my @cels = map { '─' x $cols{$_} } @colkeys;
	my $sep1 = '┌'.join('┬',@cels).'┐';
	my $sep2 = '├'.join('┼',@cels).'┤';
	my $sep3 = '└'.join('┴',@cels).'┘';
	my $vert = '│';
	#header
	my $out = "$sep1\n";
	if( $hmode ) {
		$out .= $vert;
		$out .= $self->center($_,$cols{$_}).$vert for( @colkeys );
		$out .= "\n$sep2\n";
	}
	#rows
	for my $row (@$list) {
		$out .= $vert;
		for( @colkeys ) {
			$out .= $self->left( defined $row->{$_} ? $row->{$_} : '',  $cols{$_}).$vert;
		}
		$out .= "\n";
	}
	$out .= "$sep3";
}

1
