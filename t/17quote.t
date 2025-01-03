use strict;
use warnings;

use Test::More;
use DBI;
use vars qw($test_dsn $test_user $test_password);
use lib '.', 't';
require 'lib.pl';

my $dbh;

eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1}) or ServerError() ;};

if ($@) {
    plan skip_all => "no database connection";
}

my @sqlmodes = (qw/ empty ANSI_QUOTES NO_BACKSLASH_ESCAPES/);
my @words = (qw/ foo foo'bar foo\bar /);
my @results_empty = (qw/ 'foo' 'foo\'bar' 'foo\\\\bar'/);
my @results_ansi = (qw/ 'foo' 'foo\'bar' 'foo\\\\bar'/);
my @results_no_backlslash = (qw/ 'foo' 'foo''bar' 'foo\\bar'/);
my @results = (\@results_empty, \@results_ansi, \@results_no_backlslash);

plan tests => (@sqlmodes * @words * 3 + 1);

while (my ($i, $sqlmode) = each @sqlmodes) {
  $dbh->do("SET sql_mode=?", undef,  $sqlmode eq "empty" ? "" : $sqlmode);
  for my $j (0..@words-1) {
    ok $dbh->quote($words[$j]);
    cmp_ok($dbh->quote($words[$j]), "eq", $results[$i][$j], "$sqlmode $words[$j]");

    is(
        $dbh->selectrow_array('SELECT ?', undef, $words[$j]),
        $words[$j],
        "Round-tripped '$words[$j]' through a placeholder query"
    );
  }
}

ok $dbh->disconnect;
