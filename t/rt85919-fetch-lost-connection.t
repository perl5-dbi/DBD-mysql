use strict;
use warnings;
use DBI;
use Test::More;
use lib 't', '.';
use vars qw($test_dsn $test_user $test_password $mdriver);
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });
my $sth;
my $ok = eval {
    note "Connecting...\n";
    ok( $sth = $dbh->do('SET wait_timeout = 5'), 'set wait_timeout');
    note "Sleeping...\n";
    sleep 7;
    my $sql = 'SELECT 1';
    if (1) {
        ok( $sth = $dbh->prepare($sql), 'prepare SQL');
        ok( $sth->execute(), 'execute SQL');
        my @res = $sth->fetchrow_array();
        is ( $res[0], undef, 'no rows returned');
        ok( $sth->finish(), 'finish');
        $sth = undef;
    }
    else {
        note "Selecting...\n";
        my @res = $dbh->selectrow_array($sql);
    }
    $dbh->disconnect();
    $dbh = undef;
    1;
};
if (not $ok) {
    # if we're connected via a local socket we receive error 2006
    # (CR_SERVER_GONE_ERROR) but if we're connected using TCP/IP we get 
    # 2013 (CR_SERVER_LOST)
    if ($DBI::err == 2006) {
       pass("received error 2006 (CR_SERVER_GONE_ERROR)");
    } elsif ($DBI::err == 2013) {
       pass("received error 2013 (CR_SERVER_LOST)");
    } else {
        fail('Should return error 2006 or 2013');
    }
    eval { $sth->finish(); } if defined $sth;
    eval { $dbh->disconnect(); } if defined $dbh;
}

if (0) {
  # This causes the use=after-free crash in RT #97625.
  # different testcase by killing the service. which is of course
  # not doable in a general testscript and highly system dependent.
  system(qw(sudo service mysql start));
  use DBI;
  my $dbh = DBI->connect("DBI:mysql:database=test:port=3306");
  $dbh->{mysql_auto_reconnect} = 1; # without this is works
  my $select = sub { $dbh->do(q{SELECT 1}) for 1 .. 10; };
  $select->();
  system qw(sudo service mysql stop);
  $select->();
  ok(1, "dbh did not crash on closed connection");
  system(qw(sudo service mysql start));
}

done_testing();
