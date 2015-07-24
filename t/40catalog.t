use strict;
use warnings;

use Test::More;
use DBI;
use lib '.', 't';
require 'lib.pl';
$|= 1;

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError            => 1,
                        PrintError            => 1,
                        AutoCommit            => 1,
                        mysql_server_prepare  => 0 });};

if ($@) {
    plan skip_all => "no database connection";
}
plan tests => 78;

ok(defined $dbh, "connecting");

my $sth;

#
# Bug #26604: foreign_key_info() implementation
#
# The tests for this are adapted from the Connector/J test suite.
#
SKIP: {
  skip "Server is too old to support INFORMATION_SCHEMA for foreign keys", 16
    if !MinimumVersion($dbh, '5.0');

  my $have_innodb;
  if (!MinimumVersion($dbh, '5.6')) {
    my $dummy;
    ($dummy,$have_innodb)=
      $dbh->selectrow_array("SHOW VARIABLES LIKE 'have_innodb'")
      or DbiError($dbh->err, $dbh->errstr);
  } else {
    my $engines = $dbh->selectall_arrayref('SHOW ENGINES');
    if (!$engines) {
      DbiError($dbh->err, $dbh->errstr);
    } else {
       foreach my $engine (@$engines) {
         if (lc($engine->[0]) eq 'innodb') {
           $have_innodb = $engine->[1];
         }
       }
    }
  }
  skip "Server doesn't support InnoDB, needed for testing foreign keys", 16
    unless defined $have_innodb && $have_innodb eq "YES";

  ok($dbh->do(qq{DROP TABLE IF EXISTS child, parent}), "cleaning up");

  ok($dbh->do(qq{CREATE TABLE parent(id INT NOT NULL,
                                     PRIMARY KEY (id)) ENGINE=INNODB}));
  ok($dbh->do(qq{CREATE TABLE child(id INT, parent_id INT,
                                    FOREIGN KEY (parent_id)
                                      REFERENCES parent(id) ON DELETE SET NULL)
              ENGINE=INNODB}));

  $sth= $dbh->foreign_key_info(undef, undef, 'parent', undef, undef, 'child');
  my ($info)= $sth->fetchall_arrayref({});

  is($info->[0]->{PKTABLE_NAME}, "parent");
  is($info->[0]->{PKCOLUMN_NAME}, "id");
  is($info->[0]->{FKTABLE_NAME}, "child");
  is($info->[0]->{FKCOLUMN_NAME}, "parent_id");

  $sth= $dbh->foreign_key_info(undef, undef, 'parent', undef, undef, undef);
  ($info)= $sth->fetchall_arrayref({});

  is($info->[0]->{PKTABLE_NAME}, "parent");
  is($info->[0]->{PKCOLUMN_NAME}, "id");
  is($info->[0]->{FKTABLE_NAME}, "child");
  is($info->[0]->{FKCOLUMN_NAME}, "parent_id");

  $sth= $dbh->foreign_key_info(undef, undef, undef, undef, undef, 'child');
  ($info)= $sth->fetchall_arrayref({});

  is($info->[0]->{PKTABLE_NAME}, "parent");
  is($info->[0]->{PKCOLUMN_NAME}, "id");
  is($info->[0]->{FKTABLE_NAME}, "child");
  is($info->[0]->{FKCOLUMN_NAME}, "parent_id");

  ok($dbh->do(qq{DROP TABLE IF EXISTS child, parent}), "cleaning up");
};

#
# table_info() tests
#
# These tests assume that no other tables name like 't_dbd_mysql_%' exist on
# the server we are using for testing.
#
SKIP: {
  skip "Server can't handle tricky table names", 33
    if !MinimumVersion($dbh, '4.1');

  my $sth = $dbh->table_info("%", undef, undef, undef);
  is(scalar @{$sth->fetchall_arrayref()}, 0, "No catalogs expected");

  $sth = $dbh->table_info(undef, "%", undef, undef);
  ok(scalar @{$sth->fetchall_arrayref()} > 0, "Some schemas expected");

  $sth = $dbh->table_info(undef, undef, undef, "%");
  ok(scalar @{$sth->fetchall_arrayref()} > 0, "Some table types expected");

  ok($dbh->do(qq{DROP TABLE IF EXISTS t_dbd_mysql_t1, t_dbd_mysql_t11,
                                      t_dbd_mysql_t2, t_dbd_mysqlat2,
                                      `t_dbd_mysql_a'b`,
                                      `t_dbd_mysql_a``b`}),
              "cleaning up");
  ok($dbh->do(qq{CREATE TABLE t_dbd_mysql_t1 (a INT)}) and
     $dbh->do(qq{CREATE TABLE t_dbd_mysql_t11 (a INT)}) and
     $dbh->do(qq{CREATE TABLE t_dbd_mysql_t2 (a INT)}) and
     $dbh->do(qq{CREATE TABLE t_dbd_mysqlat2 (a INT)}) and
     $dbh->do(qq{CREATE TABLE `t_dbd_mysql_a'b` (a INT)}) and
     $dbh->do(qq{CREATE TABLE `t_dbd_mysql_a``b` (a INT)}),
     "creating test tables");

  # $base is our base table name, with the _ escaped to avoid extra matches
  my $esc = $dbh->get_info(14); # SQL_SEARCH_PATTERN_ESCAPE
  (my $base = "t_dbd_mysql_") =~ s/([_%])/$esc$1/g;

  # Test fetching info on a single table
  $sth = $dbh->table_info(undef, undef, $base . "t1", undef);
  my $info = $sth->fetchall_arrayref({});

  is($info->[0]->{TABLE_CAT}, undef);
  is($info->[0]->{TABLE_NAME}, "t_dbd_mysql_t1");
  is($info->[0]->{TABLE_TYPE}, "TABLE");
  is(scalar @$info, 1, "one row expected");

  # Test fetching info on a wildcard
  $sth = $dbh->table_info(undef, undef, $base . "t1%", undef);
  $info = $sth->fetchall_arrayref({});

  is($info->[0]->{TABLE_CAT}, undef);
  is($info->[0]->{TABLE_NAME}, "t_dbd_mysql_t1");
  is($info->[0]->{TABLE_TYPE}, "TABLE");
  is($info->[1]->{TABLE_CAT}, undef);
  is($info->[1]->{TABLE_NAME}, "t_dbd_mysql_t11");
  is($info->[1]->{TABLE_TYPE}, "TABLE");
  is(scalar @$info, 2, "two rows expected");

  # Test fetching info on a single table with escaped wildcards
  $sth = $dbh->table_info(undef, undef, $base . "t2", undef);
  $info = $sth->fetchall_arrayref({});

  is($info->[0]->{TABLE_CAT}, undef);
  is($info->[0]->{TABLE_NAME}, "t_dbd_mysql_t2");
  is($info->[0]->{TABLE_TYPE}, "TABLE");
  is(scalar @$info, 1, "only one table expected");

  # Test fetching info on a single table with ` in name
  $sth = $dbh->table_info(undef, undef, $base . "a`b", undef);
  $info = $sth->fetchall_arrayref({});

  is($info->[0]->{TABLE_CAT}, undef);
  is($info->[0]->{TABLE_NAME}, "t_dbd_mysql_a`b");
  is($info->[0]->{TABLE_TYPE}, "TABLE");
  is(scalar @$info, 1, "only one table expected");

  # Test fetching info on a single table with ' in name
  $sth = $dbh->table_info(undef, undef, $base . "a'b", undef);
  $info = $sth->fetchall_arrayref({});

  is($info->[0]->{TABLE_CAT}, undef);
  is($info->[0]->{TABLE_NAME}, "t_dbd_mysql_a'b");
  is($info->[0]->{TABLE_TYPE}, "TABLE");
  is(scalar @$info, 1, "only one table expected");

  # Test fetching our tables with a wildcard schema
  # NOTE: the performance of this could be bad if the mysql user we
  # are connecting as can see lots of databases.
  $sth = $dbh->table_info(undef, "%", $base . "%", undef);
  $info = $sth->fetchall_arrayref({});

  is(scalar @$info, 5, "five tables expected");

  # Check that tables() finds and escapes the tables named with quotes
  $info = [ $dbh->tables(undef, undef, $base . 'a%') ];
  like($info->[0], qr/\.`t_dbd_mysql_a'b`$/, "table with single quote");
  like($info->[1], qr/\.`t_dbd_mysql_a``b`$/,  "table with back quote");
  is(scalar @$info, 2, "two tables expected");

  # Clean up
  ok($dbh->do(qq{DROP TABLE IF EXISTS t_dbd_mysql_t1, t_dbd_mysql_t11,
                                      t_dbd_mysql_t2, t_dbd_mysqlat2,
                                      `t_dbd_mysql_a'b`,
                                      `t_dbd_mysql_a``b`}),
              "cleaning up");
};

#
# view-related table_info tests
#
SKIP: {
  skip "Server is too old to support views", 19
  if !MinimumVersion($dbh, '5.0');

  #
  # Bug #26603: (one part) support views in table_info()
  #
  ok($dbh->do(qq{DROP VIEW IF EXISTS bug26603_v1}) and
     $dbh->do(qq{DROP TABLE IF EXISTS bug26603_t1}), "cleaning up");

  ok($dbh->do(qq{CREATE TABLE bug26603_t1 (a INT)}) and
     $dbh->do(qq{CREATE VIEW bug26603_v1 AS SELECT * FROM bug26603_t1}),
     "creating resources");

  # Try without any table type specified
  $sth = $dbh->table_info(undef, undef, "bug26603%");
  my $info = $sth->fetchall_arrayref({});
  is($info->[0]->{TABLE_NAME}, "bug26603_t1");
  is($info->[0]->{TABLE_TYPE}, "TABLE");
  is($info->[1]->{TABLE_NAME}, "bug26603_v1");
  is($info->[1]->{TABLE_TYPE}, "VIEW");
  is(scalar @$info, 2, "two rows expected");

  # Just get the view
  $sth = $dbh->table_info(undef, undef, "bug26603%", "VIEW");
  $info = $sth->fetchall_arrayref({});

  is($info->[0]->{TABLE_NAME}, "bug26603_v1");
  is($info->[0]->{TABLE_TYPE}, "VIEW");
  is(scalar @$info, 1, "one row expected");

  # Just get the table
  $sth = $dbh->table_info(undef, undef, "bug26603%", "TABLE");
  $info = $sth->fetchall_arrayref({});

  is($info->[0]->{TABLE_NAME}, "bug26603_t1");
  is($info->[0]->{TABLE_TYPE}, "TABLE");
  is(scalar @$info, 1, "one row expected");

  # Get both tables and views
  $sth = $dbh->table_info(undef, undef, "bug26603%", "'TABLE','VIEW'");
  $info = $sth->fetchall_arrayref({});

  is($info->[0]->{TABLE_NAME}, "bug26603_t1");
  is($info->[0]->{TABLE_TYPE}, "TABLE");
  is($info->[1]->{TABLE_NAME}, "bug26603_v1");
  is($info->[1]->{TABLE_TYPE}, "VIEW");
  is(scalar @$info, 2, "two rows expected");

  ok($dbh->do(qq{DROP VIEW IF EXISTS bug26603_v1}) and
     $dbh->do(qq{DROP TABLE IF EXISTS bug26603_t1}), "cleaning up");

};

#
# column_info() tests
#
SKIP: {
  ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "cleaning up");
  ok($dbh->do(qq{CREATE TABLE t1 (a INT PRIMARY KEY AUTO_INCREMENT,
                                  b INT,
                                  `a_` INT,
                                  `a'b` INT,
                                  bar INT
                                  )}), "creating table");

  #
  # Bug #26603: (one part) add mysql_is_autoincrement
  #
  $sth= $dbh->column_info(undef, undef, "t1", 'a');
  my ($info)= $sth->fetchall_arrayref({});
  is($info->[0]->{mysql_is_auto_increment}, 1);

  $sth= $dbh->column_info(undef, undef, "t1", 'b');
  ($info)= $sth->fetchall_arrayref({});
  is($info->[0]->{mysql_is_auto_increment}, 0);

  #
  # Test that wildcards and odd names are handled correctly
  #
  $sth= $dbh->column_info(undef, undef, "t1", "a%");
  ($info)= $sth->fetchall_arrayref({});
  is(scalar @$info, 3);
  $sth= $dbh->column_info(undef, undef, "t1", "a" . $dbh->get_info(14) . "_");
  ($info)= $sth->fetchall_arrayref({});
  is(scalar @$info, 1);
  $sth= $dbh->column_info(undef, undef, "t1", "a'b");
  ($info)= $sth->fetchall_arrayref({});
  is(scalar @$info, 1);

  #
  # The result set is ordered by TABLE_CAT, TABLE_SCHEM, TABLE_NAME and ORDINAL_POSITION.
  #
  $sth= $dbh->column_info(undef, undef, "t1", undef);
  ($info)= $sth->fetchall_arrayref({});
  is(join(' ++ ', map { $_->{COLUMN_NAME} } @{$info}), "a ++ b ++ a_ ++ a'b ++ bar");

  ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "cleaning up");
  $dbh->disconnect();
};


$dbh->disconnect();
