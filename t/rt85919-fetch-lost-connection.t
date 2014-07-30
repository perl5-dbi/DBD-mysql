use strict;
use warnings;
use DBI;
use Test::More;
use lib 't', '.';
use vars qw($table $test_dsn $test_user $test_password $mdriver);
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "ERROR: $@. Can't continue test";
}
my $sth;
my $ok = eval {
    print "Connecting...\n";
    ok( $sth = $dbh->do('SET wait_timeout = 5'), 'set wait_timeout');
    print "Sleeping...\n";
    sleep 7;
    my $sql = 'SELECT 1';
    if (1) {
        ok( $sth = $dbh->prepare($sql), 'prepare SQL');
        ok( $sth->execute(), 'execute SQL');
        my @res = $sth->fetchrow_array();
        is ( $res[0], undef, 'no rows returned');
        ok( $sth->finish(), 'finish');
        $sth = undef;
    }
    else {
        print "Selecting...\n";
        my @res = $dbh->selectrow_array($sql);
    }
    $dbh->disconnect();
    $dbh = undef;
    1;
};
if (not $ok) {
    is ( $DBI::err, 2006, 'Received error 2006' );
    is ( $DBI::errstr, 'MySQL server has gone away', 'Received MySQL server has gone away');
    eval { $sth->finish(); } if defined $sth;
    eval { $dbh->disconnect(); } if defined $dbh;
}

# different testcase with killing the service
system(qw(sudo service mysql start));
use DBI;
my $dbh = DBI->connect("DBI:mysql:database=test:port=3306");
$dbh->do(q{CREATE TABLE IF NOT EXISTS foo (something varchar(10) );}); 
$dbh->do(q{DELETE FROM foo;}); 
my $insert = sub { $dbh->do(q{INSERT INTO foo VALUES (?)}, undef, "hello$_") for 1 .. 10; }; 
$insert->(); 
system qw(sudo service mysql stop); 
$insert->();
ok(1, "dbh did not crash on closed connection");
system(qw(sudo service mysql start));
#mysql -e 'select * from test.foo'

done_testing();
