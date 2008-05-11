#!perl -w
# vim: ft=perl

use Test::More tests => 4;
use DBI;
use DBI::Const::GetInfoType;
use lib '.', 't';
require 'lib.pl';
use strict;
$|= 1;

use vars qw($table $test_dsn $test_user $test_password);

my $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0});
ok(defined $dbh, "Connected to database");

SKIP: {
  skip "Server doesn't report warnings", 3
    if $dbh->get_info($GetInfoType{SQL_DBMS_VER}) lt "4.1";

  my $sth;
  ok($sth= $dbh->prepare("DROP TABLE IF EXISTS no_such_table"));
  ok($sth->execute());

  is($sth->{mysql_warning_count}, 1);
};

$dbh->disconnect;
