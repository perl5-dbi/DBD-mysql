use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';
$|= 1;

use vars qw($test_dsn $test_user $test_password);
my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

plan tests => 7;

$dbh->{mysql_server_prepare}= 0;

ok(defined $dbh, "Connected to database for key info tests");

ok($dbh->do("DROP TABLE IF EXISTS dbd_mysql_keyinfo"), "Dropped table");

# Non-primary key is there as a regression test for Bug #26786.
ok($dbh->do("CREATE TABLE dbd_mysql_keyinfo (a int, b varchar(20), c int,
                                primary key (a,b(10)), key (c))"),
   "Created table dbd_mysql_keyinfo");

my $sth= $dbh->primary_key_info(undef, undef, 'dbd_mysql_keyinfo');
ok($sth, "Got primary key info");

my $key_info= $sth->fetchall_arrayref;

my $expect= [
              [ undef, undef, 'dbd_mysql_keyinfo', 'a', '1', 'PRIMARY' ],
              [ undef, undef, 'dbd_mysql_keyinfo', 'b', '2', 'PRIMARY' ],
            ];
is_deeply($key_info, $expect, "Check primary_key_info results");

is_deeply([ $dbh->primary_key(undef, undef, 'dbd_mysql_keyinfo') ], [ 'a', 'b' ],
          "Check primary_key results");

ok($dbh->do("DROP TABLE dbd_mysql_keyinfo"), "Dropped table");

$dbh->disconnect();
