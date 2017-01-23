use strict;
use warnings;

use DBI;
use Test::More;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh;
eval{$dbh = DBI->connect($test_dsn, $test_user, $test_password,
			    {RaiseError => 1});};

if ($@) {
    plan skip_all =>
        "no database connection";
}
plan tests => 19;

SKIP: {
    skip 'SET @@auto_increment_offset needs MySQL >= 5.0.2', 2 unless $dbh->{mysql_serverversion} >= 50002;
    ok $dbh->do('SET @@auto_increment_offset = 1');
    ok $dbh->do('SET @@auto_increment_increment = 1');
}

my $create = <<EOT;
CREATE TEMPORARY TABLE dbd_mysql_t31 (
  id INT(3) PRIMARY KEY AUTO_INCREMENT NOT NULL,
  name VARCHAR(64))
EOT

ok $dbh->do($create), "create dbd_mysql_t31";

my $query= "INSERT INTO dbd_mysql_t31 (name) VALUES (?)";

my $sth;
ok ($sth= $dbh->prepare($query));

ok defined $sth;

ok $sth->execute("Jochen");

is $sth->{mysql_insertid}, 1, "insert id == $sth->{mysql_insertid}";
is $dbh->{mysql_insertid}, 1, "insert id == $dbh->{mysql_insertid}";

ok $sth->execute("Patrick");

ok (my $sth2= $dbh->prepare("SELECT max(id) FROM dbd_mysql_t31"));

ok defined $sth2;

ok $sth2->execute();

my $max_id;
ok ($max_id= $sth2->fetch());

ok defined $max_id;

SKIP: {
  skip 'using libmysqlclient 5.7 or up we now have an empty dbh insertid',
    1, if $dbh->{mysql_clientversion} >= 50700;
  cmp_ok $dbh->{mysql_insertid}, '==', $max_id->[0],
    "dbh insert id $dbh->{'mysql_insertid'} == max(id) $max_id->[0] in dbd_mysql_t31";
}
cmp_ok $sth->{mysql_insertid}, '==', $max_id->[0],
  "sth insert id $sth->{'mysql_insertid'} == max(id) $max_id->[0]  in dbd_mysql_t31";

ok $sth->finish();
ok $sth2->finish();
ok $dbh->disconnect();
