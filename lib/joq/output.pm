package joq::output;

use JSON::XS;
use YAML::XS;

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
	my($self,$txt,$key) = @_;
	my $out = '';
	if($self->{mode} eq 'text') {
		$key = undef if $key && $key ne 'error';
		if( ref($txt) eq 'HASH' ) {
			$out = $self->list($key,$txt);
		} elsif( ref($txt) eq 'ARRAY' ) {
			my $sref = scalar @$txt ? ref(@$txt[0]) : undef;
			if( $sref eq 'HASH' ) {
				$out = $self->table($txt);
			} elsif( $sref eq 'ARRAY' ) {
				$out = $self->table($txt);
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
	my($self,@keys) = @_;
	my %w = (
		id    => 1,
		name  => 2,
		alias => 3,
		args  => 4,
	);
	sort { ($w{$a}||99) <=> ($w{$b}||99) } @keys;
}

my %fmtab = (
	lastend   => sub { joq::job::e2date(shift) },
	laststart => sub { joq::job::e2date(shift) },
	repeat    => sub { shift().'s' },
	delay     => sub { shift().'s' },
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
	my($self,$key,$list,$tab) = @_;
	$tab = '' unless defined $tab;
	$list = '' unless defined $list;
	my $out = '';
	if( ref($list) eq 'HASH' ) {
		my @keys = $self->orderkeys( keys %$list );
		my $klen = 0;
		foreach( @keys ) {
			$klen = length($_) if length($_) > $klen;
		}
		my $stab = $tab.($klen>5 ? ' ' x int($klen/2) : '   ').' |';
		$out = "\n" if length($tab);
		foreach my $k ( @keys ) {
			$out.=$tab.$self->right($k,$klen).': '.$self->list($k,$list->{$k},$stab);
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
	my($self,$list) = @_;
	#calc width/columns
	my %cols;
	my @colkeys;
	my $hmode = scalar @$list && ref($list->[0]) eq 'HASH';
	if( $hmode ) {
		for my $row (@$list) {
			for $cell ( keys %$row )  {
				$row->{$cell} = $self->fmt($cell,$row->{$cell});
				my $l = length( $row->{$cell} );
				$l = length($cell) if length($cell) > $l;
				$cols{$cell} = $l unless $cols{$cell} && $l < $cols{$cell};
			}
		}
		@colkeys = $self->orderkeys( keys %cols );
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
	#calc line separator
	my $sep = '+';
	$sep .= ('-' x $cols{$_}).'+' for( @colkeys );
	#header
	my $out = "$sep\n";
	if( $hmode ) {
		$out .= '|';
		$out .= $self->center($_,$cols{$_}).'|' for( @colkeys );
		$out .= "\n$sep\n";
	}
	#rows
	for my $row (@$list) {
		$out .= '|';
		for( @colkeys ) {
			$out .= $self->left( defined $row->{$_} ? $row->{$_} : '',  $cols{$_}).'|';
		}
		$out .= "\n";
	}
	$out .= "$sep";
}

1
