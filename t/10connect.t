use strict;
use warnings;

use Test::More ;
use DBI;
use DBI::Const::GetInfoType;
$|= 1;

use vars qw($test_dsn $test_user $test_password $test_db);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
  diag $@;
  plan skip_all => "no database connection";
}

ok(defined $dbh, "Connected to database");

for my $attribute ( qw(
  mysql_clientinfo
  mysql_clientversion
  mysql_serverversion
  mysql_hostinfo
  mysql_serverinfo
  mysql_stat
  mysql_protoinfo
) ) {
  ok($dbh->{$attribute}, "Value of '$attribute'");
  diag "$attribute is: ". $dbh->{$attribute};
}

my $sql_dbms_ver = $dbh->get_info($GetInfoType{SQL_DBMS_VER});
ok($sql_dbms_ver, 'get_info SQL_DBMS_VER');
diag "SQL_DBMS_VER is $sql_dbms_ver";

my $driver_ver = $dbh->get_info($GetInfoType{SQL_DRIVER_VER});
like(
  $driver_ver,
  qr/^\d{2}\.\d{2}\.\d{4}$/,
  'get_info SQL_DRIVER_VER like dd.dd.dddd'
);

like($driver_ver, qr/^04\./, 'SQL_DRIVER_VER starts with "04." (update for 5.x)');

my $info_hashref = $dbh->{mysql_dbd_stats};

ok($dbh->disconnect(), 'Disconnected');

# dbi docs state:
# The username and password can also be specified using the attributes
# Username and Password, in which case they take precedence over the $username
# and $password parameters.
# see https://rt.cpan.org/Ticket/Display.html?id=89835

eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
   { RaiseError => 1, PrintError => 1, AutoCommit => 0,
     Username => '4yZ73s9qeECdWi', Password => '64heUGwAsVoNqo' });};
ok($@, 'Username and Password attributes override');

eval {$dbh= DBI->connect($test_dsn, '4yZ73s9qeECdWi', '64heUGwAsVoNqo',
   { RaiseError => 1, PrintError => 1, AutoCommit => 0,
     Username => $test_user, Password => $test_password });};
ok(!$@, 'Username and Password attributes override');

done_testing;
