#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use lib 't', '.';
require 'lib.pl';
$|= 1;

use vars qw($table $test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      {
                          mysql_auto_reconnect  => 1,
                          RaiseError => 1,
                          PrintError => 1,
                          AutoCommit => 1 });
};

if ($@) {
    plan skip_all =>
        "ERROR: $DBI::errstr. Can't continue test";
}
my $dbh2;
eval {$dbh2= DBI->connect($test_dsn, $test_user, $test_password);};

if ($@) {
    plan skip_all =>
        "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 5;

ok(defined $dbh, "Handle 1 Connected to database");
ok(defined $dbh2, "Handle 2 Connected to database");

#kill first db connection to trigger an auto reconnect
ok ($dbh2->do('kill ' . $dbh->{'mysql_thread_id'}));

#insert a temporary delay, try uncommenting this if it's not seg-faulting at first,
# one of my initial tests without this delay didn't seg fault
sleep 1;

#ping first dbh handle to trigger auto-reconnect
$dbh->ping;

ok ($dbh);
ok ($dbh2);
