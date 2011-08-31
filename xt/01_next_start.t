use strict;
use warnings;

# Override before calling any module...
my $epoch = 0;
BEGIN {
    *CORE::GLOBAL::time = sub {
        $epoch;
    };
}

use Test::More tests => 1;
use joq::job;

# epoch for 2011-08-30T00:00:00+02:00
my $last = 1314655200;
$epoch = $last;

my $job = { 
    fixeday => 1,
    laststart => $last,
    when => {
        dayofweek   => "all",
        ndayofweek  => [ (1) x 7 ],
        ntime       => [ -7200 ],
    },
};

is( joq::job::calcnextstart( $job ), "2011-08-31 00:00:00", "job running in less than 1 second" );

done_testing;

