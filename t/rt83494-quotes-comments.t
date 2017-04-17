# Test special characters inside comments
# http://bugs.debian.org/311040
# http://bugs.mysql.com/27625

use strict;
use warnings;

use DBI;
use Test::More;

use vars qw($test_dsn $test_user $test_password $state);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

my %tests = (
  questionmark => " -- Does the question mark at the end confuse DBI::MySQL?\nselect ?",
  quote        => " -- 'Tis the quote that confuses DBI::MySQL\nSELECT ?"
);

for my $test ( sort keys %tests ) {

  my $sth = $dbh->prepare($tests{$test});
  ok($sth, 'created statement hande');
  ok($sth->execute(), 'executing');
  ok($sth->{ParamValues}, 'values');
  ok($sth->finish(), 'finish');

}

ok ($dbh->disconnect(), 'disconnecting from dbh');
done_testing;
