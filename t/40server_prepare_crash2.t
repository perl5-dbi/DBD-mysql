# This must be minimal test, even strict or Test::More can hide real crash
no warnings 'once';
use DBI;
use vars qw($test_dsn $test_user $test_password $test_db);
require "t/lib.pl";
eval {
  $dbh = DBI->connect($test_dsn, $test_user, $test_password, {RaiseError => 1, mysql_server_prepare => 1});
} or do {
  $@ = "unknown error" unless $@;
  $@ =~ s/ at \S+ line \d+\s*$//;
  print(($ENV{CONNECTION_TESTING} ? "Bail out!  " : "1..0 # SKIP ") . "no database connection: $@\n");
  exit($ENV{CONNECTION_TESTING} ? 255 : 0);
};
$sth1 = $dbh->prepare("SELECT 1");
$sth2 = $dbh->prepare("USE $test_db");
$dbh->disconnect;
$dbh = undef;
print "1..1\nok 1\n";
