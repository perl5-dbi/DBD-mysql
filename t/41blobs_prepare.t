#!perl
# vim: ft=perl
#
#   $Id: 40blobs.t 1103 2008-04-29 02:53:28Z capttofu $
#
#   This is a test for correct handling of BLOBS; namely $dbh->quote
#   is expected to work correctly.
#

#
# Thank you to Brad Choate for finding the bug that resulted in this test,
# which he kindly sent code that this test uses!
#

use strict;
use DBI;
use Test::More;

my $update_blob;
use vars qw($table $test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my ($dbh, $row);
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
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
    printf("%08lx %s\n", $i*32, unpack("H64", $b));
  }
}

my $create = <<EOT;
CREATE TABLE $table (
  id int(4),
  name text)
EOT

ok $dbh->do("DROP TABLE IF EXISTS $table"), "drop table if exists $table";

ok $dbh->do($create), "create table $table";

my $query = "INSERT INTO $table VALUES(?, ?)";
my $sth;
ok ($sth= $dbh->prepare($query));

ok defined($sth);

ok $sth->execute(1, $blob1), "inserting \$blob1";

ok $sth->finish;

ok ($sth= $dbh->prepare("SELECT * FROM $table WHERE id = 1"));

ok $sth->execute, "select from $table";

ok ($row = $sth->fetchrow_arrayref);

is @$row, 2, "two rows fetched";

is $$row[0], 1, "first row id == 1";

cmp_ok $$row[1], 'eq', $blob1, ShowBlob($blob1);

ok $sth->finish;

ok ($sth= $dbh->prepare("UPDATE $table SET name = ? WHERE id = 1"));

ok $sth->execute($blob2), 'inserting $blob2';

ok ($sth->finish);

ok ($sth= $dbh->prepare("SELECT * FROM $table WHERE id = 1"));

ok ($sth->execute);

ok ($row = $sth->fetchrow_arrayref);

is scalar @$row, 2, 'two rows';

is $$row[0], 1, 'row id == 1';

cmp_ok $$row[1], 'eq', $blob2, ShowBlob($blob2);

ok ($sth->finish);

ok $dbh->do("DROP TABLE $table"), "drop $table";

ok $dbh->disconnect;
