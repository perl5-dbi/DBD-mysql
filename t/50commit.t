#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($have_transactions $got_warning $test_dsn $test_user $test_password $table);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all =>
        "ERROR: $DBI::errstr. Can't continue test";
}

sub catch_warning ($) {
    $got_warning = 1;
}

sub num_rows($$$) {
    my($dbh, $table, $num) = @_;
    my($sth, $got);

    if (!($sth = $dbh->prepare("SELECT * FROM $table"))) {
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

$have_transactions = have_transactions($dbh);
my $engine= $have_transactions ? 'InnoDB' : 'MyISAM';

if ($have_transactions) {
  plan tests => 21;

  ok $dbh->do("DROP TABLE IF EXISTS $table"), "drop table if exists $table";
  my $create =<<EOT;
CREATE TABLE $table (
    id INT(4) NOT NULL default 0,
    name VARCHAR(64) NOT NULL default ''
) ENGINE=$engine
EOT

  ok $dbh->do($create), 'create $table';

  ok !$dbh->{AutoCommit}, "\$dbh->{AutoCommit} not defined |$dbh->{AutoCommit}|";

  $dbh->{AutoCommit} = 0;
  ok !$dbh->err;
  ok !$dbh->errstr;
  ok !$dbh->{AutoCommit};

  ok $dbh->do("INSERT INTO $table VALUES (1, 'Jochen')"),
  "insert into $table (1, 'Jochen')";

  my $msg;
  $msg = num_rows($dbh, $table, 1);
  ok !$msg;

  ok $dbh->rollback, 'rollback';

  $msg = num_rows($dbh, $table, 0);
  ok !$msg;

  ok $dbh->do("DELETE FROM $table WHERE id = 1"), "delete from $table where id = 1";

  $msg = num_rows($dbh, $table, 0);
  ok !$msg;
  ok $dbh->commit, 'commit';

  $msg = num_rows($dbh, $table, 0);
  ok !$msg;

  # Check auto rollback after disconnect
  ok $dbh->do("INSERT INTO $table VALUES (1, 'Jochen')");

  $msg = num_rows($dbh, $table, 1);
  ok !$msg;

  ok $dbh->disconnect;

  ok ($dbh = DBI->connect($test_dsn, $test_user, $test_password));

  ok $dbh, "connected";

  $msg = num_rows($dbh, $table, 0);
  ok !$msg;

  ok $dbh->{AutoCommit}, "\$dbh->{AutoCommit} $dbh->{AutoCommit}";

}
else {
  plan tests => 13;

  ok $dbh->do("DROP TABLE IF EXISTS $table"), "drop table if exists $table";
  my $create =<<EOT;
  CREATE TABLE $table (
      id INT(4) NOT NULL default 0,
      name VARCHAR(64) NOT NULL default ''
      ) ENGINE=$engine
EOT

  ok $dbh->do($create), 'create $table';

  # Tests for databases that don't support transactions
  # Check whether AutoCommit mode works.

  ok $dbh->do("INSERT INTO $table VALUES (1, 'Jochen')");
  my $msg = num_rows($dbh, $table, 1);
  ok !$msg;

  ok $dbh->disconnect;

  ok ($dbh = DBI->connect($test_dsn, $test_user, $test_password));

  $msg = num_rows($dbh, $table, 1);
  ok !$msg;

  ok $dbh->do("INSERT INTO $table VALUES (2, 'Tim')");

  my $result;
  $@ = '';

  $SIG{__WARN__} = \&catch_warning;

  $got_warning = 0;

  eval { $result = $dbh->commit; };

  $SIG{__WARN__} = 'DEFAULT';

  ok $got_warning;

#   Check whether rollback issues a warning in AutoCommit mode
#   We accept error messages as being legal, because the DBI
#   requirement of just issuing a warning seems scary.
  ok $dbh->do("INSERT INTO $table VALUES (3, 'Alligator')");

  $@ = '';
  $SIG{__WARN__} = \&catch_warning;
  $got_warning = 0;
  eval { $result = $dbh->rollback; };
  $SIG{__WARN__} = 'DEFAULT';

  ok $got_warning, "Should be warning defined upon rollback of non-trx table";

  ok $dbh->do("DROP TABLE $table");
  ok $dbh->disconnect();
}
