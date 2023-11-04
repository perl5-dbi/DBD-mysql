use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
  diag $@;
  plan skip_all => "no database connection";
}
plan tests => 2;

# https://github.com/perl5-dbi/DBD-mysql/issues/352
# Calling prepare on a disconnected handle causes the call to mysql_real_escape_string to segfault

my $sth;
ok $dbh->disconnect;
my $result = eval {
  $dbh->prepare('SELECT ?');
};
ok !$result
