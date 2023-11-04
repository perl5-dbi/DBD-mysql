use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

plan tests => 2;

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 0, PrintError => 1, AutoCommit => 0 });};

if ($@) {
  diag $@;
  plan skip_all => "no database connection";
}

# https://github.com/perl5-dbi/DBD-mysql/issues/352
# Calling prepare on a disconnected handle causes the call to mysql_real_escape_string to segfault

my $sth;
ok $dbh->disconnect;
ok !$dbh->prepare('SELECT ?');

