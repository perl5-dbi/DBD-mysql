#!perl -w
#
#   $Id$ 
#


use DBI ();
use DBI::Const::GetInfoType;
use Test::More;
use Data::Dumper;
use lib 't', '.';
require 'lib.pl';
use vars qw($table $test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 1 });};
if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
if ($dbh->get_info($GetInfoType{SQL_DBMS_VER}) lt "4.1") {
    plan skip_all => 
        "SKIP TEST: You must have MySQL version 4.1 and greater for this test to run";
}

plan tests => 41;

ok ($dbh->do("DROP TABLE IF EXISTS $table"));

my $create = <<EOT;
CREATE TABLE $table (
        id int(4) NOT NULL default 0,
        name varchar(64) default ''
        )
EOT

ok ($dbh->do($create));

ok ($sth = $dbh->prepare("INSERT INTO $table VALUES (?, ?)"));

# Automatic type detection
my $numericVal = 1;
my $charVal = "Alligator Descartes";
ok ($sth->execute($numericVal, $charVal));

# Does the driver remember the automatically detected type?
ok ($sth->execute("3", "Jochen Wiedmann"));

$numericVal = 2;
$charVal = "Tim Bunce";
ok ($sth->execute($numericVal, $charVal));

# Now try the explicit type settings
ok ($sth->bind_param(1, " 4", SQL_INTEGER()));

# umlaut equivelant is vowel followed by 'e'
ok ($sth->bind_param(2, 'Andreas Koenig'));
ok ($sth->execute);

# Works undef -> NULL?
ok ($sth->bind_param(1, 5, SQL_INTEGER()));

ok ($sth->bind_param(2, undef));

ok ($sth->execute);

ok ($sth->bind_param(1, undef, SQL_INTEGER()));

ok ($sth->bind_param(2, undef));

ok ($sth->execute(-1, "abc"));

ok ($dbh->do("INSERT INTO $table VALUES (6, '?')"));

ok ($dbh->do('SET @old_sql_mode = @@sql_mode, @@sql_mode = \'\''));

ok ($dbh->do("INSERT INTO $table VALUES (7, \"?\")"));

ok ($dbh->do('SET @@sql_mode = @old_sql_mode'));

ok ($sth = $dbh->prepare("SELECT * FROM $table ORDER BY id"));

ok($sth->execute);

ok ($sth->bind_columns(undef, \$id, \$name));

$ref = $sth->fetch ; 

is $id,  -1, 'id set to -1'; 

cmp_ok $name, 'eq', 'abc', 'name eq abc'; 

$ref = $sth->fetch;
is $id, 1, 'id set to 1';
cmp_ok $name, 'eq', 'Alligator Descartes', '$name set to Alligator Descartes';

$ref = $sth->fetch;
is $id, 2, 'id set to 2';
cmp_ok $name, 'eq', 'Tim Bunce', '$name set to Tim Bunce';

$ref = $sth->fetch;
is $id, 3, 'id set to 3';
cmp_ok $name, 'eq', 'Jochen Wiedmann', '$name set to Jochen Wiedmann';

$ref = $sth->fetch;
is $id, 4, 'id set to 4';
cmp_ok $name, 'eq', 'Andreas Koenig', '$name set to Andreas Koenig';

$ref = $sth->fetch;
is $id, 5, 'id set to 5';
ok !defined($name), 'name not defined';

$ref = $sth->fetch;
is $id, 6, 'id set to 6';
cmp_ok $name, 'eq', '?', "\$name set to '?'";

$ref = $sth->fetch;
is $id, 7, '$id set to 7';
cmp_ok $name, 'eq', '?', "\$name set to '?'";

ok ($dbh->do("DROP TABLE $table"));

ok $sth->finish;

ok $dbh->disconnect;
