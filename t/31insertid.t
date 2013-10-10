#!/usr/bin/perl

use strict;
use warnings;

use strict;
use DBI ();
use Test::More;
use Data::Dumper;

use vars qw($table $test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh;
eval{$dbh = DBI->connect($test_dsn, $test_user, $test_password,
			    {RaiseError => 1});};

if ($@) {
    plan skip_all =>
        "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 18;

ok $dbh->do('SET @@auto_increment_offset = 1');
ok $dbh->do('SET @@auto_increment_increment = 1');

my $create = <<EOT;
CREATE TEMPORARY TABLE $table (
  id INT(3) PRIMARY KEY AUTO_INCREMENT NOT NULL,
  name VARCHAR(64))
EOT

ok $dbh->do($create), "create $table";

my $query= "INSERT INTO $table (name) VALUES (?)";

my $sth;
ok ($sth= $dbh->prepare($query));

ok defined $sth;

ok $sth->execute("Jochen");

is $dbh->{'mysql_insertid'}, 1, "insert id == $dbh->{mysql_insertid}";

ok $sth->execute("Patrick");

ok (my $sth2= $dbh->prepare("SELECT max(id) FROM $table"));

ok defined $sth2;

ok $sth2->execute();

my $max_id;
ok ($max_id= $sth2->fetch());

ok defined $max_id;

cmp_ok $sth->{'mysql_insertid'}, '==', $max_id->[0], "sth insert id $sth->{'mysql_insertid'} == max(id) $max_id->[0]  in $table";

cmp_ok $dbh->{'mysql_insertid'}, '==', $max_id->[0], "dbh insert id $dbh->{'mysql_insertid'} == max(id) $max_id->[0] in $table";

ok $sth->finish();

ok $sth2->finish();

ok $dbh->disconnect();
