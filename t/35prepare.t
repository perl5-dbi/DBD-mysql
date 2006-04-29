#!/usr/bin/perl

#use strict;
use DBI;
use Carp qw(croak);
use Data::Dumper;

$^W =1;

$test_dsn = 'DBD::mysql:database=test;host=localhost:mysql_server_prepare=1';
$test_user = '';
$test_password = '';
$table = "testtable";

use DBI;
$mdriver = "";
foreach $file ("lib.pl", "t/lib.pl", "DBD-mysql/t/lib.pl") {
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

while(Testing()) {
  Test($state or $dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})) or ServerError() ;

  # don't want this during make test!
  #Test($state or $dbh->trace("3", "/tmp/trace.log")) or DbiError($dbh->err, $dbh->errstr);


#
#   Create a new table; EDIT THIS!
#


Test($state or $dbh->do("DROP TABLE IF EXISTS $table")) or DbiError($dbh->err, $dbh->errstr);

Test($state or ($def = TableDefinition($table,
  ["id",   "INTEGER",  4, 0],
  ["name", "CHAR",    64, 0]),
  $dbh->do($def)))
  or DbiError($dbh->err, $dbh->errstr);

  #
  # test SHOW command - 'prepare' should not be used (check db log)
  # 
  Test($state or $cursor = $dbh->prepare("SHOW TABLES LIKE '$table'"))
        or DbiError($dbh->err, $dbh->errstr);
  Test($state or $cursor->execute())
	   or DbiError($dbh->err, $dbh->errstr);
  my ($row, $errstr, $vers, $supported);
  Test(
    $state or 
    (defined($row= $cursor->fetchrow_arrayref)  &&
    (!defined($errstr = $cursor->errstr) || $cursor->errstr eq '')))
         or DbiError($cursor->err, $cursor->errstr);

  Test ($state or ($row->[0] eq "$table")) 
      or print "results not equal to '$table' \n";

  print "inserting values into $table using 'do' emulated.\n";
  my $no_bind_insert = "INSERT INTO $table VALUES (1,'1st first value')";
  Test($state or $sth = $dbh->do($no_bind_insert)) or 
    DbiError($dbh->err, $dbh->errstr);

  print "inserting values into $table without placeholders.\n";
  $no_bind_insert = "INSERT INTO $table VALUES (1,'2nd second value')";
  Test($state or $sth = $dbh->prepare($no_bind_insert)) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $rows = $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->finish) or 
    DbiError($dbh->err, $dbh->errstr);

  print "Performing test select on $table.\n";
  my $no_bind_query = "SELECT id, name FROM $table WHERE id = 1";
  Test($state or $sth = $dbh->prepare($no_bind_query)) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $retRef = $sth->fetchall_arrayref()
    &&
    print "Dumper of \$retRef: " . Dumper $retRef) or
    DbiError($dbh->err, $dbh->errstr);

	my $bind_insert = "INSERT INTO $table values (?, ?)";
	
	# create the table
	
	Test($state or $sth = $dbh->prepare($bind_insert))
    or DbiError($dbh->err, $dbh->errstr);
	
	my $testInsertVals = {};
	for (my $i = 0 ; $i < 10; $i++) { 
	  my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
	  my $random_chars = join '', map { $chars[rand @chars] } 0 .. 16;
	  # save these values for later testing
	  $testInsertVals->{$i} = $random_chars;
	  Test($state or $rows = $sth->execute($i, $random_chars) && print "rows  $rows\n")
      or DbiError($dbh->err, $dbh->errstr);
	}
  Test($state or $sth->finish) or 
    DbiError($dbh->err, $dbh->errstr);

	my $whole_table_query = "SELECT * FROM $table WHERE id = ? OR id = ?";  
  Test($state or $sth = $dbh->prepare($whole_table_query)) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $rows = $sth->execute(1,2)) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $retRef = $sth->fetchall_arrayref()) or  
    DbiError($dbh->err, $dbh->errstr);

	my $drop = "DROP TABLE IF EXISTS $table";
	Test($state or $sth = $dbh->prepare($drop)) or
    DbiError($dbh->err, $dbh->errstr);

	Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

    $drop= "DROP TABLE IF EXISTS t1";
    Test($state or $sth = $dbh->prepare($drop)) or
      DbiError($dbh->err, $dbh->errstr);

    Test($state or $sth->execute()) or 
      DbiError($dbh->err, $dbh->errstr);

	$create = "CREATE TABLE t1 (a int)";
	Test($state or $sth = $dbh->prepare($create)) or
    DbiError($dbh->err, $dbh->errstr);

	Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

  my $alter= "ALTER TABLE t1 ADD COLUMN b varchar(32)";
  Test($state or $sth = $dbh->prepare($alter)) or
    DbiError($dbh->err, $dbh->errstr);

	Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

  $bind_insert= "INSERT INTO t1 VALUES (?, ?), (?,?)"; 
  Test($state or $sth =
   $dbh->prepare($bind_insert)) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute(1, "first val", 2, "second val")) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth= 
    $dbh->prepare("select version()")) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or 
    (defined($row= $sth->fetchrow_arrayref)  &&
    (!defined($errstr = $sth->errstr) || $sth->errstr eq '')))
         or DbiError($cursor->err, $cursor->errstr);

    if ($row->[0] !~ /^4\.0/ || $row->[0] !~ /^3/) {
      $state= 1;
    }

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

    $drop = "DROP PROCEDURE IF EXISTS testproc";

    Test($state or $dbh->prepare($drop)) or
      DbiError($dbh->err, $dbh->errstr);

    Test($state or $sth->execute()) or 
      DbiError($dbh->err, $dbh->errstr);

    $drop = "DROP TABLE IF EXISTS t1";
    Test($state or $sth= $dbh->prepare($drop)) or
      DbiError($dbh->err, $dbh->errstr);

    Test($state or $sth->execute()) or 
      DbiError($dbh->err, $dbh->errstr);

}
