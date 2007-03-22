#!perl -w
# vim: ft=perl

use Test::More tests => 111;
use DBI;
use DBI::Const::GetInfoType;
use strict;
$|= 1;

my $rows = 0;
my $sth;
my $testInsertVals;
our ($test_dsn, $test_user, $test_password, $mdriver);
$mdriver='';
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
ok(defined $dbh, "Connected to database");

ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "making slate clean");

ok($dbh->do(qq{CREATE TABLE t1 (id INT(4), name VARCHAR(64))}), "creating table");

ok(($sth = $dbh->prepare("INSERT INTO t1 VALUES (?,?)")));

print "PERL testing insertion of values from previous prepare of insert statement:\n";
for (my $i = 0 ; $i < 100; $i++) { 
  my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
  my $random_chars = join '', map { $chars[rand @chars] } 0 .. 16;
# save these values for later testing
  $testInsertVals->{$i} = $random_chars;
  ok(($rows = $sth->execute($i, $random_chars)));
}
print "PERL rows : " . $rows . "\n"; 

#print "PERL testing prepare of select statement with INT and VARCHAR placeholders:\n";
#ok(($sth = $dbh->prepare("SELECT * FROM t1 WHERE id = ? AND name = ?")));

#for my $id (keys %$testInsertVals) {
#  print "id $id value $testInsertVals->{$id}\n";
#  $sth->execute($id, $testInsertVals->{$id});
#}
     
print "PERL testing prepare of select statement with LIMIT placeholders:\n";
ok($sth = $dbh->prepare("SELECT * FROM t1 LIMIT ?, ?"));

print "PERL testing exec of bind vars for LIMIT\n";
ok($sth->execute(20, 50));

my ($row, $errstr, $array_ref);
ok( (defined($array_ref = $sth->fetchall_arrayref) &&
  (!defined($errstr = $sth->errstr) || $sth->errstr eq '')));

ok(@$array_ref == 50);

ok($sth->finish);

#
#   Finally drop the test table.
#
ok($dbh->do("DROP TABLE t1"));

ok($dbh->disconnect);


