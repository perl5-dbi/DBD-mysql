use strict;
use warnings;

use Test::More ;
use DBI;
use DBI::Const::GetInfoType;
use vars qw($mdriver);
$|= 1;

use vars qw($test_dsn $test_user $test_password $test_db);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
  plan skip_all => "no database connection";
}

ok(defined $dbh, "Connected to database");

for my $attribute ( qw(mysql_clientinfo mysql_clientversion mysql_serverversion) ) {
  ok($dbh->{$attribute}, "Value of '$attribute'");
  diag "$attribute is: ", $dbh->{$attribute};
}

my $v= $dbh->get_info($GetInfoType{SQL_DBMS_VER});
diag "SQL_DBMS_VER: $v";

ok($dbh->disconnect(), 'Disconnected');

done_testing;
