use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh1 = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, AutoCommit => 0 });

my $dbh2 = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, AutoCommit => 0 });

my @ilwtenabled = $dbh1->selectrow_array("SHOW VARIABLES LIKE 'innodb_lock_wait_timeout'");
if (!@ilwtenabled) {
  plan skip_all => 'innodb_lock_wait_timeout not available';
}

my $have_innodb = 0;
if (!MinimumVersion($dbh1, '4.1.2')) {
  my $dummy;
  ($dummy,$have_innodb)=
    $dbh1->selectrow_array("SHOW VARIABLES LIKE 'have_innodb'")
    or DbiError($dbh1->err, $dbh1->errstr);
} else {
  my $engines = $dbh1->selectall_arrayref('SHOW ENGINES');
  if (!$engines) {
    DbiError($dbh1->err, $dbh1->errstr);
  } else {
     STORAGE_ENGINE:
     for my $engine (@$engines) {
       next STORAGE_ENGINE if lc $engine->[0] ne 'innodb';
       next STORAGE_ENGINE if lc $engine->[1] eq 'no';
       $have_innodb = 1;
     }
  }
}
if (!$have_innodb) {
  plan skip_all => "Server doesn't support InnoDB, needed for testing innodb_lock_wait_timeout";
}

eval {
  $dbh2->{PrintError} = 0;
  $dbh2->do("SET innodb_lock_wait_timeout=1");
  $dbh2->{PrintError} = 1;
  1;
} or do {
  $dbh1->disconnect();
  $dbh2->disconnect();
  plan skip_all => "innodb_lock_wait_timeout is not modifyable on this version of MySQL";
};

ok $dbh1->do("DROP TABLE IF EXISTS dbd_mysql_rt75353_innodb_lock_timeout"), "drop table if exists dbd_mysql_rt75353_innodb_lock_timeout";
ok $dbh1->do("CREATE TABLE dbd_mysql_rt75353_innodb_lock_timeout(id INT PRIMARY KEY) ENGINE=INNODB"), "create table dbd_mysql_rt75353_innodb_lock_timeout";

ok $dbh1->do("INSERT INTO dbd_mysql_rt75353_innodb_lock_timeout VALUES(1)"), "dbh1: acquire a row lock on table dbd_mysql_rt75353_innodb_lock_timeout";

my $error_handler_called = 0;
$dbh2->{HandleError} = sub { $error_handler_called = 1; die $_[0]; };
eval { $dbh2->selectcol_arrayref("SELECT id FROM dbd_mysql_rt75353_innodb_lock_timeout FOR UPDATE") };
my $error_message = $@;
$dbh2->{HandleError} = undef;
ok $error_message, "dbh2: acquiring same lock as dbh1 on table dbd_mysql_rt75353_innodb_lock_timeout failed";

like $error_message, qr/Lock wait timeout exceeded; try restarting transaction/, "dbh2: error message for acquiring lock is 'Lock wait timeout exceeded'";
ok $error_handler_called, "dbh2: error handler code ref was called";

$dbh2->disconnect();

ok $dbh1->do("DROP TABLE dbd_mysql_rt75353_innodb_lock_timeout"), "drop table dbd_mysql_rt75353_innodb_lock_timeout";
$dbh1->disconnect();

done_testing;
