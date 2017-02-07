use strict;
use warnings;

use DBI;
use Test::More;

my $update_blob;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1});

plan tests => 25;

my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
my $blob1= join '', map { $chars[rand @chars] } 0 .. 10000;
my $blob2 = '"' x 10000;

sub ShowBlob($) {
  my ($blob) = @_;
  my $b;
  for(my $i = 0;  $i < 8;  $i++) {
    if (defined($blob)  &&  length($blob) > $i) {
      $b = substr($blob, $i*32);
    }
    else {
      $b = "";
    }
    note sprintf("%08lx %s\n", $i*32, unpack("H64", $b));
  }
}

my $create = <<EOT;
CREATE TABLE dbd_mysql_41blobs_prepare (
  id int(4),
  name text)
EOT

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_41blobs_prepare"), "drop table if exists dbd_mysql_41blobs_prepare";

ok $dbh->do($create), "create table dbd_mysql_41blobs_prepare";

my $query = "INSERT INTO dbd_mysql_41blobs_prepare VALUES(?, ?)";
my $sth;
ok ($sth= $dbh->prepare($query));

ok defined($sth);

ok $sth->execute(1, $blob1), "inserting \$blob1";

ok $sth->finish;

ok ($sth= $dbh->prepare("SELECT * FROM dbd_mysql_41blobs_prepare WHERE id = 1"));

ok $sth->execute, "select from dbd_mysql_41blobs_prepare";

ok (my $row = $sth->fetchrow_arrayref);

is @$row, 2, "two rows fetched";

is $$row[0], 1, "first row id == 1";

cmp_ok $$row[1], 'eq', $blob1, ShowBlob($blob1);

ok $sth->finish;

ok ($sth= $dbh->prepare("UPDATE dbd_mysql_41blobs_prepare SET name = ? WHERE id = 1"));

ok $sth->execute($blob2), 'inserting $blob2';

ok ($sth->finish);

ok ($sth= $dbh->prepare("SELECT * FROM dbd_mysql_41blobs_prepare WHERE id = 1"));

ok ($sth->execute);

ok ($row = $sth->fetchrow_arrayref);

is scalar @$row, 2, 'two rows';

is $$row[0], 1, 'row id == 1';

cmp_ok $$row[1], 'eq', $blob2, ShowBlob($blob2);

ok ($sth->finish);

ok $dbh->do("DROP TABLE dbd_mysql_41blobs_prepare"), "drop dbd_mysql_41blobs_prepare";

ok $dbh->disconnect;
