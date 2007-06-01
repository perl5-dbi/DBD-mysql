#!perl -w
# vim: ft=perl

use Test::More tests => 7;
use DBI;
use strict;
$|= 1;

my $mdriver= "";
our ($test_dsn, $test_user, $test_password);
foreach my $file ("lib.pl", "t/lib.pl") {
  do $file;
  if ($@) {
    print STDERR "Error while executing $file: $@\n";
    exit 10;
  }
  last if $mdriver ne '';
}

my $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

$dbh->{mysql_server_prepare}= 0;


ok(defined $dbh, "Connected to database for key info tests");

ok($dbh->do(qq{DROP TABLE IF EXISTS t1}),
   "Dropped table");

# Non-primary key is there as a regression test for Bug #26786.
ok($dbh->do(qq{CREATE TABLE t1 (a int, b varchar(20), c int,
                                primary key (a,b(10)), key (c))}),
   "Created table");

my $sth= $dbh->primary_key_info(undef, undef, 't1');
ok($sth, "Got primary key info");

my $key_info= $sth->fetchall_arrayref;

my $expect= [
              [ undef, undef, 't1', 'a', '1', 'PRIMARY' ],
              [ undef, undef, 't1', 'b', '2', 'PRIMARY' ],
            ];
is_deeply($key_info, $expect, "Check primary_key_info results");

is_deeply([ $dbh->primary_key(undef, undef, 't1') ], [ 'a', 'b' ],
          "Check primary_key results");

ok($dbh->do(qq{DROP TABLE t1}), "Dropped table");

$dbh->disconnect();
