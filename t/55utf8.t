#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use Carp qw(croak);
use Test::More;
use vars qw($table $test_dsn $test_user $test_password);
use vars qw($COL_NULLABLE $COL_KEY);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "ERROR: $@. Can't continue test";
}

#
# DROP/CREATE PROCEDURE will give syntax error for these versions
#
if (!MinimumVersion($dbh, '5.0')) {
    plan skip_all =>
        "SKIP TEST: You must have MySQL version 5.0 and greater for this test to run";
}
plan tests => 16 * 2;

for my $mysql_server_prepare (0, 1) {
$dbh= DBI->connect($test_dsn . ';mysql_server_prepare=' . $mysql_server_prepare, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

ok $dbh->do("DROP TABLE IF EXISTS $table");

my $create =<<EOT;
CREATE TABLE $table (
    name VARCHAR(64) CHARACTER SET utf8,
    bincol BLOB,
    shape GEOMETRY,
    binutf VARCHAR(64) CHARACTER SET utf8 COLLATE utf8_bin,
    profile TEXT CHARACTER SET utf8
)
EOT

ok $dbh->do($create);

my $utf8_str        = "\x{0100}dam";     # "Adam" with a macron.
my $quoted_utf8_str = "'\x{0100}dam'";

my $blob = "\x{c4}\x{80}dam"; # same as utf8_str but not utf8 encoded
my $quoted_blob = "'\x{c4}\x{80}dam'";

cmp_ok $dbh->quote($utf8_str), 'eq', $quoted_utf8_str, 'testing quoting of utf 8 string';

cmp_ok $dbh->quote($blob), 'eq', $quoted_blob, 'testing quoting of blob';

#ok $dbh->{mysql_enable_utf8}, "mysql_enable_utf8 survive connect()";
$dbh->{mysql_enable_utf8}=1;

my $query = <<EOI;
INSERT INTO $table (name, bincol, shape, binutf, profile)
    VALUES (?, ?, GeomFromText('Point(132865 501937)'), ?, ?)
EOI

ok $dbh->do($query, {}, $utf8_str, $blob, $utf8_str, $utf8_str), "INSERT query $query\n";

$query = "SELECT name,bincol,asbinary(shape), binutf, profile FROM $table LIMIT 1";
my $sth = $dbh->prepare($query) or die "$DBI::errstr";

ok $sth->execute;

my $ref;
$ref = $sth->fetchrow_arrayref ;

ok defined $ref;

cmp_ok $ref->[0], 'eq', $utf8_str;

cmp_ok $ref->[3], 'eq', $utf8_str;
cmp_ok $ref->[4], 'eq', $utf8_str;

SKIP: {
        eval {use Encode;};
          skip "Can't test is_utf8 tests 'use Encode;' not available", 2, if $@;
          ok !Encode::is_utf8($ref->[1]), "blob was made utf8!.";

          ok !Encode::is_utf8($ref->[2]), "shape was made utf8!.";
      }

cmp_ok $ref->[1], 'eq', $blob, "compare $ref->[1] eq $blob";

ok $sth->finish;

ok $dbh->do("DROP TABLE $table");

ok $dbh->disconnect;
}
