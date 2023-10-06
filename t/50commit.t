use strict;
use warnings;

use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($got_warning $test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "no database connection";
}

sub catch_warning ($) {
    $got_warning = 1;
}

sub num_rows($$$) {
    my($dbh, $table, $num) = @_;
    my($sth, $got);

    if (!($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t50commit"))) {
      return "Failed to prepare: err " . $dbh->err . ", errstr "
        . $dbh->errstr;
    }
    if (!$sth->execute) {
      return "Failed to execute: err " . $dbh->err . ", errstr "
        . $dbh->errstr;
    }
    $got = 0;
    while ($sth->fetchrow_arrayref) {
      ++$got;
    }
    if ($got ne $num) {
      return "Wrong result: Expected $num rows, got $got.\n";
    }
    return '';
}

plan tests => 22;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t50commit"), "drop table if exists dbd_mysql_t50commit";
my $create =<<EOT;
CREATE TABLE dbd_mysql_t50commit (
  id INT(4) NOT NULL default 0,
  name VARCHAR(64) NOT NULL default ''
) ENGINE=InnoDB
EOT

ok $dbh->do($create), 'create dbd_mysql_t50commit';

ok !$dbh->{AutoCommit}, "\$dbh->{AutoCommit} not defined |$dbh->{AutoCommit}|";

$dbh->{AutoCommit} = 0;
ok !$dbh->err;
ok !$dbh->errstr;
ok !$dbh->{AutoCommit};

ok $dbh->do("INSERT INTO dbd_mysql_t50commit VALUES (1, 'Jochen')"),
"insert into dbd_mysql_t50commit (1, 'Jochen')";

my $msg;
$msg = num_rows($dbh, 'dbd_mysql_t50commit', 1);
ok !$msg;

ok $dbh->rollback, 'rollback';

$msg = num_rows($dbh, 'dbd_mysql_t50commit', 0);
ok !$msg;

ok $dbh->do("DELETE FROM dbd_mysql_t50commit WHERE id = 1"), "delete from dbd_mysql_t50commit where id = 1";

$msg = num_rows($dbh, 'dbd_mysql_t50commit', 0);
ok !$msg;
ok $dbh->commit, 'commit';

$msg = num_rows($dbh, 'dbd_mysql_t50commit', 0);
ok !$msg;

# Check auto rollback after disconnect
ok $dbh->do("INSERT INTO dbd_mysql_t50commit VALUES (1, 'Jochen')");

$msg = num_rows($dbh, 'dbd_mysql_t50commit', 1);
ok !$msg;

ok $dbh->disconnect;

ok ($dbh = DBI->connect($test_dsn, $test_user, $test_password));

ok $dbh, "connected";

$msg = num_rows($dbh, 'dbd_mysql_t50commit', 0);
ok !$msg;

ok $dbh->{AutoCommit}, "\$dbh->{AutoCommit} $dbh->{AutoCommit}";
ok $dbh->do("DROP TABLE dbd_mysql_t50commit");
