#!perl -w
#
#   $Id: 40bindparam.t 6127 2008-10-08 22:36:13Z zhur $ 
#


use DBI ();
use DBI::Const::GetInfoType;
use Test::More;
use lib 't', '.';
require 'lib.pl';
use vars qw($table $test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
if ($dbh->get_info($GetInfoType{SQL_DBMS_VER}) lt "4.1") {
    plan skip_all => 
        "SKIP TEST: You must have MySQL version 4.1 and greater for this test to run";
}

plan tests => 17;

ok ($dbh->do("DROP TABLE IF EXISTS $table"));

my $create = <<EOT;
CREATE TABLE $table (
        id int(4) NOT NULL default 0,
        name varchar(100) default ''
        )
EOT

ok ($dbh->do($create));

ok ($sth = $dbh->prepare("INSERT INTO $table (name, id)" .
           " VALUES ('Charles de Batz de Castelmore, comte d\\'Artagnan', ?)"));

ok ($sth->execute(1));

ok ($sth = $dbh->prepare("INSERT INTO $table (name, id)" .
                         " VALUES ('Charles de Batz de Castelmore, comte d\\'Artagnan', 2)"));

ok ($sth->execute());

ok ($sth = $dbh->prepare("INSERT INTO $table (name, id) VALUES (?, ?)"));

ok ($sth->execute("Charles de Batz de Castelmore, comte d\\'Artagnan", 3));

ok ($sth = $dbh->prepare("INSERT INTO $table (id, name)" .
                         " VALUES (?, 'Charles de Batz de Castelmore, comte d\\'Artagnan')"));

ok ($sth->execute(1));

ok ($sth = $dbh->prepare("INSERT INTO $table (id, name)" .
                         " VALUES (2, 'Charles de Batz de Castelmore, comte d\\'Artagnan')"));

ok ($sth->execute());

ok ($sth = $dbh->prepare("INSERT INTO $table (id, name) VALUES (?, ?)"));

ok ($sth->execute(3, "Charles de Batz de Castelmore, comte d\\'Artagnan"));

ok ($dbh->do("DROP TABLE $table"));

ok $sth->finish;

ok $dbh->disconnect;
