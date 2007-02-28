#!perl -w
# vim: ft=perl

use Test::More;
use DBI;
use strict;
$|= 1;

my $mdriver= "";

our ($test_dsn, $test_user, $test_password);
foreach my $file ("lib.pl", "t/lib.pl") {
  do $file;
  if ($@) {
    print STDERR "Error while executing $file: $@\n";
    exit 10;
  }
  last if $mdriver ne '';
}

my $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

plan tests => 24;

ok(defined $dbh, "connecting");

my $sth;

my ($version)= $dbh->selectrow_array("SELECT version()")
  or DbiError($dbh->err, $dbh->errstr);

#
# Bug #26604: foreign_key_info() implementation
#
# The tests for this are adapted from the Connector/J test suite.
#
SKIP: {
  skip "Server is too old to support INFORMATION_SCHEMA for foreign keys", 16
    if substr($version, 0, 1) < 5;

  my ($have_innodb)= $dbh->selectrow_array("SELECT \@\@have_innodb = 'YES'")
    or DbiError($dbh->err, $dbh->errstr);
  skip "Server doesn't support InnoDB, needed for testing foreign keys", 16
    if not $have_innodb;

  ok($dbh->do(qq{DROP TABLE IF EXISTS child, parent}), "cleaning up");

  ok($dbh->do(qq{CREATE TABLE parent(id INT NOT NULL,
                                     PRIMARY KEY (id)) ENGINE=INNODB}));
  ok($dbh->do(qq{CREATE TABLE child(id INT, parent_id INT,
                                    FOREIGN KEY (parent_id)
                                      REFERENCES parent(id) ON DELETE SET NULL)
              ENGINE=INNODB}));

  $sth= $dbh->foreign_key_info(undef, undef, "parent", undef, undef, "child");
  my ($info)= $sth->fetchall_arrayref({});

  is($info->[0]->{PKTABLE_NAME}, "parent");
  is($info->[0]->{PKCOLUMN_NAME}, "id");
  is($info->[0]->{FKTABLE_NAME}, "child");
  is($info->[0]->{FKCOLUMN_NAME}, "parent_id");

  $sth= $dbh->foreign_key_info(undef, undef, "parent", undef, undef, undef);
  ($info)= $sth->fetchall_arrayref({});

  is($info->[0]->{PKTABLE_NAME}, "parent");
  is($info->[0]->{PKCOLUMN_NAME}, "id");
  is($info->[0]->{FKTABLE_NAME}, "child");
  is($info->[0]->{FKCOLUMN_NAME}, "parent_id");

  $sth= $dbh->foreign_key_info(undef, undef, undef, undef, undef, "child");
  ($info)= $sth->fetchall_arrayref({});

  is($info->[0]->{PKTABLE_NAME}, "parent");
  is($info->[0]->{PKCOLUMN_NAME}, "id");
  is($info->[0]->{FKTABLE_NAME}, "child");
  is($info->[0]->{FKCOLUMN_NAME}, "parent_id");

  ok($dbh->do(qq{DROP TABLE IF EXISTS child, parent}), "cleaning up");
};

#
# Bug #26603: support views in table_info(), add primary_key_info(),
# add mysql_is_autoincrement
#
SKIP: {
  skip "Server is too old to support views", 16
    if substr($version, 0, 1) < 5;

  ok($dbh->do(qq{DROP VIEW IF EXISTS v1}) and
     $dbh->do(qq{DROP TABLE IF EXISTS t1}), "cleaning up");

  ok($dbh->do(qq{CREATE TABLE t1 (a INT)}) and
     $dbh->do(qq{CREATE VIEW v1 AS SELECT * FROM t1}), "creating resources");

  $sth= $dbh->table_info(undef, undef, undef);
  my ($info)= $sth->fetchall_arrayref({});

  is($info->[0]->{TABLE_NAME}, "t1");
  is($info->[0]->{TABLE_TYPE}, "TABLE");
  is($info->[1]->{TABLE_NAME}, "v1");
  is($info->[1]->{TABLE_TYPE}, "VIEW");

  ok($dbh->do(qq{DROP VIEW IF EXISTS v1}) and
     $dbh->do(qq{DROP TABLE IF EXISTS t1}), "cleaning up");

};


$dbh->disconnect();
