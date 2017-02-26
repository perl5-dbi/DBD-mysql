use strict;
use warnings;

use Test::More;
use DBI;
use DBI::Const::GetInfoType;
$|= 1;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
        {   RaiseError => 1,
            PrintError => 1,
            AutoCommit => 0,
            mysql_init_command => 'SET SESSION wait_timeout=7' });

plan tests => 5;

ok(defined $dbh, "Connected to database");

ok(my $sth=$dbh->prepare("SHOW SESSION VARIABLES like 'wait_timeout'"));

ok($sth->execute());

ok(my @fetchrow = $sth->fetchrow_array());

is($fetchrow[1],'7','session variable is 7');

$sth->finish();

$dbh->disconnect();

