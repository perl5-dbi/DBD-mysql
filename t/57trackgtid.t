use strict;
use warnings;

use DBI;
use Test::More;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh;
eval{$dbh = DBI->connect($test_dsn, $test_user, $test_password,
			    {RaiseError => 1});};

if ($@) {
    plan skip_all =>
        "no database connection";
}

if ($dbh->{mysql_serverversion} > 100000) {
    plan skip_all => "GTID tracking is not available on MariaDB";
}

if ($dbh->{mysql_serverversion} < 50000) {
    plan skip_all => "You must have MySQL version 5.0.0 and greater for this test to run";
}

my @gtidtrackenabled = $dbh->selectrow_array('select @@global.session_track_gtids');
if (!@gtidtrackenabled) {
  plan skip_all => 'GTID tracking not available';
} elsif ($gtidtrackenabled[0] eq 'OFF') {
  plan skip_all => 'GTID tracking not enabled';
} else {
  plan tests => 2;
}

$dbh->do('FLUSH PRIVILEGES');
cmp_ok(length($dbh->{'mysql_gtids'}),'>=',38);

ok $dbh->disconnect();
