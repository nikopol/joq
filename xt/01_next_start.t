use strict;
use warnings;

# Override before calling any module...
my $epoch = 0;
BEGIN {
    *CORE::GLOBAL::time = sub {
        $epoch;
    };
}

use Test::More; # tests => 26;
use joq::job;
use DateTime;
use RTGI::DateTimeHandler;
use YAML;

is( time, 0 );
my $dth = RTGI::DateTimeHandler->new;

#my $d = DateTime->from_epoch( epoch=>$e, time_zone=>$cfg{timezone} );

my $last;

$last = $dth->parse_datetime( "2011-08-30T00:00:00" )->epoch;
$epoch = $last;
print Dump { 'last' => $dth->format_epoch( $last ), 'time' => $dth->format_epoch( $epoch ) };

my $job = { 
    fixeday => 1,
    laststart => $last,
    when => {
        dayofweek   => "all",
        ndayofweek  => [ (1) x 7 ],
        ntime       => [ -7200 ],
    },
};

print Dump joq::job::calcnextstart( $job );
print Dump $job;

done_testing;

#---
#when:
  #dayofweek: all
  #ndayofweek:
    #- 1
    #- 1
    #- 1
    #- 1
    #- 1
    #- 1
    #- 1
  #ntime:
    #- 51360
  #start: 2011-08-30 16:16:00
  #time: 16:16


