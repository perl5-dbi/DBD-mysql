#!/usr/bin/perl

use strict;
use vars qw($test_dsn $test_user $test_password $mdriver $state);
use DBI;
use Carp qw(croak);
use Data::Dumper;
$^W =1;

use DBI;
$mdriver = "";
my ($row, $vers, $test_procs, $dbh, $sth);
foreach my $file ("lib.pl", "t/lib.pl", "DBD-mysql/t/lib.pl") {
  do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
    exit 10;
  }
  if ($mdriver ne '') {
    last;
  }
}

sub ServerError() {
    print STDERR ("Cannot connect: ", $DBI::errstr, "\n",
	"\tEither your server is not up and running or you have no\n",
	"\tpermissions for acessing the DSN $test_dsn.\n",
	"\tThis test requires a running server and write permissions.\n",
	"\tPlease make sure your server is running and you have\n",
	"\tpermissions, then retry.\n");
    exit 10;
}

$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1}) or ServerError() ;

$sth= $dbh->prepare("select version()") or
  DbiError($dbh->err, $dbh->errstr);

$sth->execute() or 
  DbiError($dbh->err, $dbh->errstr);

$row= $sth->fetchrow_arrayref() or
  DbiError($dbh->err, $dbh->errstr);

# 
# DROP/CREATE PROCEDURE will give syntax error 
# for these versions
#
if ($row->[0] =~ /^5/)
{
  $test_procs= 1;
}
$sth->finish();
$dbh->disconnect();

if (! $test_procs)
{
  print "1..0 # Skip MySQL Server version doesn't support stored procedures\n";
  exit(0);
}

while(Testing())
{
  my $table;
  Test($state or $dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})) or ServerError() ;

  # don't want this during make test!
  Test($state or (1 || $dbh->trace("3", "/tmp/trace.log"))) or
  DbiError($dbh->err, $dbh->errstr);

  Test($state or $table = FindNewTable($dbh))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $dbh->do("DROP TABLE IF EXISTS $table"))
    or DbiError($dbh->err, $dbh->errstr);

  my $drop_proc= "DROP PROCEDURE IF EXISTS testproc";
  Test($state or ($sth = $dbh->prepare($drop_proc))) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or ($sth->execute())) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or ($sth= $dbh->do($drop_proc))) or 
    DbiError($dbh->err, $dbh->errstr);

  my $proc_create = <<EOPROC;
create procedure testproc() deterministic
  begin
    declare a,b,c,d int;
    set a=1;
    set b=2;
    set c=3;
    set d=4;
    select a, b, c, d;
    select d, c, b, a;
    select b, a, c, d;
    select c, b, d, a;
  end
EOPROC

  Test($state or $sth = $dbh->prepare($proc_create)) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

#
#    my $proc_call = 'CALL testproc()';
#    Test(($state && !$supported) or $sth = $dbh->prepare($proc_call)) or
#    DbiError($dbh->err, $dbh->errstr);
#
#    Test($state or $sth->execute()) or 
#      DbiError($dbh->err, $dbh->errstr);
#
#    my $proc_select = 'SELECT @a';
#    Test($state or $sth = $dbh->prepare($proc_select)) or
#    DbiError($dbh->err, $dbh->errstr);
#
#    Test($state or $sth->execute()) or 
#      DbiError($dbh->err, $dbh->errstr);

  Test($state or ($sth=$dbh->prepare("DROP PROCEDURE testproc"))) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);
}
