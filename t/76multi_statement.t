#!perl -w
# vim: ft=perl

use strict;
use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use lib 't', '.';
require 'lib.pl';
$|= 1;

use vars qw($table $test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0,
                        mysql_multi_statements => 1 });};

if ($@) {
    plan skip_all => "ERROR: $@. Can't continue test";
}
plan tests => 7; 

ok(defined $dbh, "Connected to database with multi statement support");

$dbh->{mysql_server_prepare}= 0;

SKIP: {
  skip "Server doesn't support multi statements", 6 
    if $dbh->get_info($GetInfoType{SQL_DBMS_VER}) lt "4.1";

  ok($dbh->do("DROP TABLE IF EXISTS $table"), "clean up");
  ok($dbh->do("CREATE TABLE $table (a INT)"), "create table");

  ok($dbh->do("INSERT INTO $table VALUES (1); INSERT INTO $table VALUES (2);"));

  $dbh->disconnect();

  $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                     { RaiseError => 0, PrintError => 0, AutoCommit => 0,
                       mysql_multi_statements => 0 });
  ok(defined $dbh, "Connected to database without multi statement support");

  ok(not $dbh->do("INSERT INTO $table VALUES (1); INSERT INTO $table VALUES (2);"));

  ok($dbh->do("DROP TABLE IF EXISTS $table"), "clean up");
};

$dbh->disconnect();
