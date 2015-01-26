use strict;
use warnings;

use DBI;
use DBI::Const::GetInfoType;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval { $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1,
                        PrintError => 1, 
                        AutoCommit => 0,
                        mysql_bind_comment_placeholders => 1,}
                        );
     };
if ($@) {
    plan skip_all => 
        "no database connection";
}

my $create= <<"EOTABLE";
CREATE TEMPORARY TABLE dbd_mysql_53 (
    id bigint unsigned not null default 0
    )
EOTABLE


ok $dbh->do($create), "creating table";

my $statement= "insert into dbd_mysql_53 (id) values (?)";

my $sth;
ok $sth= $dbh->prepare($statement);

my $rows;
ok $rows= $sth->execute('1');
cmp_ok $rows, '==',  1;
$sth->finish();


my $retrow;

if ( $test_dsn =~ m/mysql_server_prepare=1/ ) {
    # server_prepare can't bind placeholder on comment.
    ok 1;
    ok 2;
}
else {
$statement= <<EOSTMT;
SELECT id 
FROM dbd_mysql_53
-- this comment has ? in the text 
WHERE id = ?
EOSTMT
    $retrow= $dbh->selectrow_arrayref($statement, {}, 'hey', 1);
    cmp_ok $retrow->[0], '==', 1;

    $statement= "SELECT id FROM dbd_mysql_53 /* Some value here ? */ WHERE id = ?";

    $retrow= $dbh->selectrow_arrayref($statement, {}, "hello", 1);
    cmp_ok $retrow->[0], '==', 1;
}


$statement= "SELECT id FROM dbd_mysql_53 WHERE id = ? ";
my $comment = "/* it's/a_directory/does\ this\ work/bug? */";
$statement= $statement . $comment;

for (0 .. 9) {
    $retrow= $dbh->selectrow_arrayref($statement, {}, 1);
    cmp_ok $retrow->[0], '==', 1;
}

$comment = "/* $0 */";

for (0 .. 9) {
    $retrow= $dbh->selectrow_arrayref($statement . $comment, {}, 1);
    cmp_ok $retrow->[0], '==', 1;
}

ok $dbh->disconnect;

done_testing;
