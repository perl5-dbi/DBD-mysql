#!perl -w
# vim: ft=perl
# Test problem in 3.0002_4 and 3.0005 where if a statement is prepared
# and multiple executes are performed, if any execute fails all subsequent
# executes report an error but may have worked.

use strict;
use DBI ();
use DBI::Const::GetInfoType;
use Test::More;
use lib '.', 't';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

$test_dsn.= ";mysql_server_prepare=1";
my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})};

if ($@) {
    plan skip_all => "ERROR: $@. Can't continue test";
}

# 
# DROP/CREATE PROCEDURE will give syntax error 
# for versions < 5.0
#
if ($dbh->get_info($GetInfoType{SQL_DBMS_VER}) lt "4.1") {
    plan skip_all => 
        "SKIP TEST: You must have MySQL version 4.1 and greater for this test to run";
}
plan tests => 3;

# execute invalid SQL to make sure we get an error
my $q = "select select select";	# invalid SQL
$dbh->{PrintError} = 0;
$dbh->{PrintWarn} = 0;
my $sth;
eval {$sth = $dbh->prepare($q);};
$dbh->{PrintError} = 1;
$dbh->{PrintWarn} = 1;
ok defined($DBI::errstr);
cmp_ok $DBI::errstr, 'ne', '';

print "errstr $DBI::errstr\n" if $DBI::errstr;
ok $dbh->disconnect();
