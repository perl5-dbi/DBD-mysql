#!/usr/bin/perl


require 5.003;
use strict;

$| = 1;

# Variables we're going to use
my(
   $class,
   $errstrRef,
   $query,
   $table1,
   $table2,
   $dbh,
   $dbh2,
   $dbh3,
   $him,
   $sth,
   $i,
   @row,
   %hash,
  );

my($file);
foreach $file ("lib.pl", "t/lib.pl", "Mysql/t/lib.pl") {
    if (-f $file) {
	do $file;
	if ($@) { die "Cannot load 'lib.pl': $@.\n"; }
	last;
    }
}

use vars qw($mdriver);
if ($mdriver ne 'mysql'  &&  $mdriver ne 'mSQL'  &&  $mdriver ne 'mSQL1') {
    print "1..0\n";
    exit 0;
}

$::listTablesHook = $::listTablesHook = sub ($) {
    my($dbh) = shift; $dbh->listtables;
};


# Running the testscript with a hostname as $ARGV[0] runs the test via
# a TCP socket. Per default we connect to the unix socket to avoid
# problems you might have with resolving "localhost". Too many systems
# are configured wrong in this respect. But you're welcome to test it
# out.

my $host = shift @ARGV || $ENV{'DBI_HOST'}
  || $::test_host || $::test_host;  # Make -w happy
my $port = shift @ARGV || $ENV{'DBI_PORT'}
  || $::test_port || $::test_port;  # Make -w happy
$host .= ":$port" if $port;
my $user = shift @ARGV || $ENV{'DBI_USER'}
  || $::test_user || $::test_user;   # Make -w happy
my $password = shift @ARGV || $ENV{'DBI_PASS'}
  || $::test_password || $::test_password;     # Make -w happy
my $dbname = shift @ARGV || $ENV{'DBI_DB'}
  || $::test_db || $::test_db;

use vars qw($mdriver $state $COL_NULLABLE $COL_KEY $testNum);
if ($mdriver eq 'mysql') {
    $class = 'Mysql';
    eval "use $class";
    $Mysql::db_errstr = '';
    $errstrRef = \$Mysql::db_errstr;
} elsif ($mdriver eq 'mSQL1') {
    $class = 'Msql1';
    eval "use $class";
    $Msql1::db_errstr = '';
    $errstrRef = \$Msql1::db_errstr;
} else {
    $class = 'Msql';
    eval "use $class";
    $Msql::db_errstr = '';
    $errstrRef = \$Msql::db_errstr;
}


sub ServerError() {
    $$errstrRef ||= "";
    my $onhost = $host ? " (on $host)" : "";
    print STDERR ("Cannot connect: $$errstrRef\n",
	"\tIt looks as if your server$onhost is not up and running.\n",
	"\tThis test requires a running server.\n",
	"\tPlease make sure your server is running and retry.\n");
    exit 10;
}

sub DatabaseError() {
    print STDERR ("Cannot select database 'test': $$errstrRef.\n",
	"Please make sure that a database \"$dbname\" exists\n",
	"and that you have permission to read and write on it.\n");
    exit 10;
}


# For the error messages we're going to produce within this script we
# write a subroutine, so the typical error message will always look
# more or less similar:

sub test_error {
    my($dbh,$id,$query,$error) = @_;
    $id    ||= "?";               # Newer Test::Harness will accept that
    $query ||= "";                # query is optional
    $query = "\n\tquery $query" if $query;
    $error ||= $dbh->errmsg;      # without error we ask Msql
    print "\terrmsg $error$query\n";
}


sub unctrl {
    my $str = shift;
    $str =~ s/([\000-\037\177])/ '^' . pack('c', ord($1) ^ 64) /eg;
    return $str;
}

sub create {
    my($db,$tablename,$createexpression) = @_;
    my($query) = "create table $tablename $createexpression";
    my $limit = 0;
    while (!$db->query($query)){
	die "Cannot create table: query [$query] message"
	    . " [$$errstrRef]\n" if $limit++ > 1000;
	$tablename++;
	$query = "create table $tablename $createexpression";
    }
    $tablename;
}

sub drop {shift->query("drop table $_[0]"); }


while (Testing()) {
    # You may connect in two steps: (1) Connect and (2) SelectDB...

    Test($state or ($dbh = $class->connect($host, $dbname, $user, $password)), undef,
	 "First connect to server")
	or ServerError();

    Test($state or $dbh->selectdb($dbname))
	or DatabaseError();

    Test($state or ((undef $dbh) or 1));

    # Or you may call connect with two arguments, the first being the
    # host, and the second being the DB

    Test($state or
	 ($dbh = $class->connect($host,$dbname, $user, $password)), undef,
	 "Trying two argument connect")
	or print("Error while connecting: $$errstrRef.\n");

    Test($state or $dbh->listtables or !$$errstrRef)
	or print("Error while listing tables: $$errstrRef.\n");

    # Now we create two tables that are certainly not in the test database
    # If you don't understand the trickery here, just skip this section,
    # No big deal.

    if (!$state) {
	if (($table1 = FindNewTable($dbh)) eq '') {
	    print "Cannot determine name of first test table: $$errstrRef.\n";
	    exit 10;
	}
	if (($table2 = FindNewTable($dbh)) eq '') {
	    print "Cannot determine name of second test table: $$errstrRef.\n";
	    exit 10;
	}
    } else {
	$table1 = '';  # Suppress warnings for undefined variables
	$table2 = '';
    }

    Test($state or ($query = TableDefinition($table1,
				     ["she", "CHAR",  32, $COL_NULLABLE],
				     ["him", "CHAR",  32, 0],
				     ["who", "CHAR",  32, $COL_NULLABLE])))
	or print("Cannot get table definition.\n");

    Test($state or $dbh->query($query), undef, "Creating first test table")
	or (print "Cannot create first table: $$errstrRef.\n", exit);

    Test($state or ($query = TableDefinition($table2,
				     ["she", "CHAR",  32, $COL_NULLABLE],
				     ["him", "CHAR",  32, 0],
				     ["who", "CHAR",  32, $COL_NULLABLE])))
	or print("Cannot get table definition.\n");

    Test($state or $dbh->query($query))
	or (print "Cannot create second table: $$errstrRef.\n", exit);

    Test($state or $dbh->listtables)
	or print("Error while listing tables: $$errstrRef.\n");

    # Now we write some test records into the two tables. Note, we *know*,
    # these tables are empty

    for $query (
        "insert into $table1 values ('Anna', 'Franz', 'Otto')",
	"insert into $table1 values ('Sabine', 'Thomas', 'Pauline')"  ,
	"insert into $table1 values ('Jane', 'Paul', 'Jah')"	      ,
	"insert into $table2 values ('Henry', 'Francis', 'James')"   ,
	"insert into $table2 values ('Cashrel', 'Beco', 'Lotic')"
		) {
	Test($state or $dbh->query($query)) or test_error($dbh,0,$query);
    }

    $query = "select * from $table1";
    Test($state or ($sth = $dbh->query($query)), undef, "First SELECT")
	or test_error($dbh,0,$query);
    Test($state or ($sth->numrows == 3))
	or printf("Wrong number of rows, expected %d, got %d.\n",
			      3, $sth->numrows);
    if ($mdriver eq 'mysql') {
	Test($state or ($sth->numfields == 3))
	    or printf("Wrong number of fields, expected %d,"
				   . " got %d.\n",
				   3, $sth->numrows);
    }

    # There is the array reference $sth->name. It has to have as many
    # fields as $sth->numfields tells us
    Test($state or (@{$sth->name} == $sth->numfields), undef,
	 'Checking $sth->name')
	or printf("Wrong number of names, expected %d, got %d.\n",
		  $sth->numfields, @{$sth->name});

    # There is the array reference $sth->table. We expect, that all three
    # fields in the array have the same value, as we only selected from
    # $table1
    Test($state or ($sth->table->[0] eq $table1), undef,
	 'Checking $sth->table')
	or printf("Wrong table name, expected %s, got %s.\n",
		  $table1, $sth->table->[0]);
    Test($state or ($sth->table->[1] eq $table1))
	or printf("Wrong table name, expected %s, got %s.\n",
			       $table1, $sth->table->[1]);
    Test($state or ($sth->table->[2] eq $table1))
	or printf("Wrong table name, expected %s, got %s.\n",
		  $table1, $sth->table->[2]);

    # CHAR_TYPE, NUM_TYPE and REAL_TYPE are exported functions from
    # Msql. That is why you have to say 'use Msql'. The functions are
    # really constants, but that's the way headerfile constants are
    # handled in perl5 up to 5.001m (will probably change soon)
    my ($expected);
    if (!$state) {
	if ($mdriver eq 'mysql') {
	    $expected = Mysql::FIELD_TYPE_STRING();
	} elsif ($mdriver eq 'mSQL1') {
	    $expected = Msql1::CHAR_TYPE();
	} else {
	    $expected = Msql::CHAR_TYPE();
	}
    }
    Test($state or ($sth->type->[0] eq $expected), undef,
	 'Checking $sth->type')
	or printf("Wrong result type, expected %d, got %d.\n",
		  $expected, $sth->type->[0]);

    # Now we count the rows ourselves, we don't trust anybody
    my $rowcnt=0;
    if (!$state) {
	while (@row = $sth->fetchrow()){
	    $rowcnt++;
	}
    }
    Test($state or ($rowcnt == $sth->numrows))
	or printf("Counted wrong number of rows, expected %d,"
		  . " got %d.\n",
		  $sth->numrows, $rowcnt);

    # We haven't yet tested DataSeek, so lets count again
    if (!$state) {
	$rowcnt=0;
	$sth->dataseek(0);
	while (@row = $sth->fetchrow()){
	    $rowcnt++;
	}
    }
    Test($state or ($rowcnt == $sth->numrows))
	or printf("Counted wrong number of rows after"
		  . " dataseek, expected %d, got %d.\n",
		  $sth->numrows, $rowcnt);

    # let's see the second table
    Test($state or ($sth = $dbh->query("select * from $table2")))
	or test_error($dbh);

    # We set the second field "not null". Does the API know that?
    Test($state or ($sth->is_not_null->[1] > 0), undef,
	 'Checking $sth->is_not_null')
	or printf("NOT NULL not recognized (%s).\n",
		  join(" ", @{$sth->is_not_null}));

    # Are we able to just reconnect with the *same* scalar ($dbh) playing
    # the role of the db-handle?
    Test($state or ($dbh = $class->connect($host,$dbname, $user, $password)))
	or print("Error while reconnecting: $$errstrRef.\n");

    # We may have an arbitrary number of statementhandles. Each
    # statementhandle consumes memory, so in reality we try to scope them
    # with my() within a block or we reuse them or we undef them.
    {
	# Declare the statement handle as lexically scoped (see man
	# perlfunc and search for 'my EXPR') Don't forget to scope other
	# variables too, that you won't need outside the block
	my($sth1,$sth2,@row1,$count);

	Test($state or ($sth1 = $dbh->query("select * from $table1")),
	     undef, 'Checking second sth')
	    or print("Query had some problem:"
		     . " $$errstrRef\n");
	Test($state or ($sth2 = $dbh->query("select * from $table2")))
	    or print("Query had some problem:"
		     . " $$errstrRef\n");

	# You have seen this above, so NO COMMENT :)
	$count=0;
	if (!$state) {
	    while ($sth2->fetchrow  and  (@row1 = $sth1->fetchrow)) {
		$count++;
	    }
	}
	Test($state or ($count == 2))
	    or printf("Mismatch with two statement handles,"
		      . " expected %d, got %d rows.\n",
		      2, $count);

	# When we undef this handle, the memory associated with it is
	# freed
	Test($state or undef ($sth2) or 1);

	if (!$state) {
	    $count=0;
	    while (@row1 = $sth1->fetchrow){
		$count++;
	    }
	}
	Test($state or ($count == 1))
	    or printf("Row mismatch with first statement handle,"
		      . " expected %d, got %d.\n",
		      1, $count);

	# When we leave this block, the memory associated with $sth1 is
	# freed
    }

    # What happens, when we have errors?
    # Yes, there's a typo: we add a paren to the statement
    {
	# The use of the -w switch is really a good idea in general, but
	# if you want the -w switch but do NOT want to see mysql's error
	# messages, you can turn them off using $mysql::QUIET

	# In reality we would say "or die ...", but in this case we forgot it:
	if (!$state) {
	    local($Mysql::QUIET) = 1;  # Doesn't hurt to set both ... :-)
	    local($Msql::QUIET) = 1;
	    local($Msql1::QUIET) = 1;

	    $sth = $dbh->query("select * from $table1 where him = 'Thomas')");
	}

	# $mysql::db_errstr should contain the word "error" now
	Test($state or ($dbh->errmsg =~ /error/), undef,
	     'Forcing error message')
	    or printf("Expected error message.\n");
    }

    # Now $sth should be undefined, because the query above failed. If we
    # try to use this statementhandle, we should die. We don't want to
    # die, because we are in a test script. So we check what happens with
    # eval
    if (!$state) {
	eval "\@row = \$sth->fetchrow;";
    }
    Test($state or $@)
	or printf("Expected driver to die with error message.\n");

    # Remember, we inserted a row into table $table1 ('Sabine',
    # 'Thomas', 'Pauline'). Let's see, if they are still there.
    Test($state or ($sth = $dbh->query("select * from $table1"
				       . " where him = 'Thomas'")))
	or print("Query had some problem: $$errstrRef.\n");

    Test($state or (@row = $sth->fetchrow))
	or print("$table1 didn't find a matching row");

    Test($state or ($row[2] eq "Pauline"))
	or print("Expected 'Pauline' being in the"
			      . " second field.\n");

    {
	# %fieldnum is a hash that associates the index number for each field
	# name:
	my %fieldnum;

	if (!$state) {
	    @fieldnum{@{$sth->name}} = 0..@{$sth->name}-1;
	}

	# %fieldnum is now (she => 0, him => 1, who => 2)

	# So we do not have to hard-code the zero for "she" here

	Test($state or ($row[$fieldnum{"she"}] eq 'Sabine'))
	    or print("Expected 'she' being 'Sabine'.\n");
    }

    # After 18 tests, the database handle may feel the desire to rest. Or
    # maybe the writer of this script has forgotten, that he is already
    # connected

    # While in reality you should use your database connections
    # economically -- they cost you a slot in the server connection table,
    # and you can easily run out of available slots -- we, in the test
    # script want to know what happens with more than one handle
    Test($state or ($dbh2 = $class->connect($host,$dbname,$user,$password)), undef,
	 'Reconnect')
	or print("Error while reconnecting: $$errstrRef.\n");

    # Some quick checks about the contents of the handle...
    Test($state or ($dbh2->database eq $dbname))
	or printf("Error in database name, expected %s,"
		  . " got %s.\n",
		  $dbname, $dbh2->database);
    if (!$state) {
	$i = ($mdriver eq 'mysql') ? $dbh2->sockfd : $dbh2->sock;
    }

    Test($state or ($i =~ /^\d+$/))
	or printf("Expected socket number being an integer,"
			       . " got %s.\n", $i);

    # Is $dbh2 able to drop a table, while we are connected with $dbh?
    # Sure it can...
    Test($state or $dbh2->query("drop table $table2"), undef,
	 'Second dbh')
	or print("Error while dropping table with second handle:"
			      . " $$errstrRef.\n");

    {
	# Does ListDBs find the test database? Sure...
	my @array;
	if (!$state) {
	    @array = $dbh2->listdbs;
	}

	Test($state or (grep( /^$dbname$/, @array )), undef, 'ListDBs')
	    or print("'test' database not in db list.\n");

	# Does ListTables now find our $table1?
	if (!$state) {
	    @array = $dbh2->listtables;
	}
	Test($state or (grep( /^$table1$/, @array )))
	    or printf("'$table1' not in table list.\n");
    }

    # The third connection within a single script. I promise, this will do...
    Test($state or ($dbh3 = Connect $class($host,$dbname,$user,$password)), undef,
	 'Third connection')
	or test_error($dbh3, $testNum);

    Test($state or ($dbh3->host eq $host))
	or printf("Wrong host name, expected %s, got %s.\n",
			       $host, $dbh3->host);
    Test($state or ($dbh3->database eq $dbname))
	or printf("Wrong database name, expected %s, got %s.\n",
			       $dbname, $dbh3->database);

    # For what it's worth, we have a tough job for the server here. First
    # we define two simple subroutines. The goal of these is to make the
    # create table statement independent of what happens on the server
    # side. If the table cannot be created we magic increment the
    # suggested name and retry. We return the incremented table name. With
    # this setting we can run the test script in parallel in many
    # processes.

    # Then we insert some nonsense changing the dbhandle quickly
    if (!$state) {
	my $C="AAAA"; 
	my $N=1;
	drop($dbh2,$table1);
	$table1 = create($dbh2,$table1,"( name char(40) not null,
            num int, country char(4), mytime real )");

	for (1..5){
	    $dbh2->query("insert into $table1 values
	        ('".$C++."',".$N++.",'".$C++."',".rand().")")
		or test_error($dbh2);
	    $dbh3->query("insert into $table1 values
	        ('".$C++."',".$N++.",'".$C++."',".rand().")")
		or test_error($dbh2);
	}
    }


    # I haven't shown you yet a cute (and dirty) trick to save memory. As
    # ->query returns an object you can reference this object in a single
    # chain of -> operators. The statement handle is not preserved, and
    # the memory associated with it is cleaned up within a single
    # statement. 'Course you never know, which part of the statement
    # failed--if something fails.

    Test($state or (($i = $dbh2->query("select * from $table1")->numrows)
		    == 10))
	 or printf("Expected parallel query to produce %d rows,"
		   . " got %d.\n", 10, $i);

    # Interesting the following test. Creating and dropping of tables via
    # two different database handles in quick alteration. There was really
    # a version of mSQL that messed up with this

    if (!$state) {
	for (1..3){
	    drop($dbh2,$table1);
	    $table2 = create($dbh3,$table2,"( name char(40) not null,
                 num int, country char(4), mytime real )");
	    drop($dbh2,$table2);
	    $table1 = create($dbh3,$table1,"( name char(40) not null,
                num int, country char(4), mytime real )");
	}
    }
    Test($state or drop($dbh2,$table1))
	 or print("Error in create/drop alteration:"
		  . " $$errstrRef\n");

    # A quick check, if the array @{$sth->length} is available and
    # correct. See man perlref for an explanation of this kind of
    # referencing/dereferencing. Watch out, that we still use an old
    # statement handle here. The corresponding table has been overwritten
    # quite a few times, but as we are dealing with an in-memeory copy, we
    # still have it available
    Test($state or ("@{$sth->length}" eq "32 32 32"), undef,
	 'Checking $sth->length')
	 or printf("Error in length array, expected %s,"
				. " got %s.\n",
				"32 32 32", "@{$sth->length}");

    # The following tests show, that NULL fields (introduced with
    # msql-1.0.6) are handled correctly:
    Test($state or ($query = TableDefinition($table1,
				     ["she", "CHAR",    14, 0],
				     ["him", "INTEGER", 4,  $COL_NULLABLE],
				     ["who", "CHAR",    1,  $COL_NULLABLE])))
	or print("Cannot get table definition.\n");
    Test($state or $dbh->query($query))  or  test_error($dbh, 0, $query);

    # As you see, we don't insert a value for "him" and "who", so we can
    # test the undefinedness
    $query = "insert into $table1 (she) values ('jazz')";
    Test($state or $dbh->query($query))  or  test_error($dbh, 0, $query);
    $query = "select * from $table1";
    Test($state or ($sth = $dbh->query($query)))
	 or test_error($dbh, 0, $query);
    Test($state or (@row = $sth->fetchrow()))
	 or test_error($dbh);

    # "she" is "jazz", thusly defined
    Test($state or defined($row[0]))
	or printf("Expected 'she' being 'jazz', got 'undef'.\n");

    # field "him", a character field, should not be defined

    Test($state or !defined($row[1]))
	or printf("Expected 'him' being 'undef', got '%s'.\n",
			       $row[1]);

    # field "who", an integer field, should not be defined

    Test($state or !defined($row[2]))
	or printf("Expected 'who' being 'undef', got '%s'.\n",
			       $row[1]);

    # If we only select a field that will be undefined when we
    # call fetchrow, we should nontheless have a TRUE fetchrow

    my $sth3;
    print "Verifying whether fetchrow returns TRUE for results.\n";
    $query = "select him from $table1";
    Test($state or ($sth3 = $dbh->query($query)))
	 or test_error($dbh, 0, $query);

    # "him" is undef, but fetchrow is TRUE

    Test($state or (($him) = $sth3->fetchrow) > 0)
	or print("Expected fetchrow() returning TRUE:"
			      . " $$errstrRef.\n");
    Test($state or !defined($him))
	or printf("Expected 'him' being 'undef', got '%s'.\n",
			       $him);

    # So far we have evaluated metadata in scalar context. Let's see,
    # if array context works

    foreach (qw/table name type is_not_null is_pri_key length/) {
	my @arr;
	if (!$state) {
	    @arr = $sth->$_();
	}
	Test($state or (@arr == 3))
	    or printf("Error in array context of $_: got %s.\n",
				   "@arr");
    }

    # A non-select should return TRUE, and if anybody tries to use this
    # return value as an object reference, we should not core dump

    {
	local($Mysql::QUIET, $Msql::QUIET, $Msql1::QUIET) = (1, 1, 1);

	Test($state or (($sth) = $dbh->query("insert into $table1 values"
					     . " (\047x\047,2,\047y\047)")))
	    or test_error($dbh);
	use vars qw($ref);
	if (!$state) {
	    eval '$ref = $sth->fetchrow;';
	}
	if ($mdriver eq 'mysql') {
	    Test($state or ($@ eq ''), undef,
		 "Fetchrow from non-select handle $sth")
		or printf("Died while fetching a row from a"
			  . " non-result handle, error was $@.\n");
	} else {
	    Test($state or ($@ ne ''), undef,
		 "Fetchrow from non-select handle")
		or print("Fetching a row from a non-result handle",
			 " without dying.\n");
	    Test($state or ($@ =~ /without a package or object/))
		or printf("Fetching row from a non-result handle"
			  . " produced wrong error message $@.\n");
	}

	Test($state or !defined($ref))
	    or printf("Fetching a row from a non-result handle"
		      . " returned TRUE ($ref).\n");
    }

    {
	my($sth_query, $sth_listf, $method, $ok);

	# So many people have problems using the ListFields method,
	# so we finally provide a simple example.
	Test($state or ($sth_query = $dbh->query("select * from $table1")))
	    or test_error($dbh);
	Test($state or ($sth_listf = $dbh->listfields($table1)))
	    or test_error($dbh);
	for $method (qw/name table length type is_not_null is_pri_key/) {
	    $ok = 1;
	    if (!$state) {
		for (0..$sth_query->numfields -1) {
		    # whatever we do to the one statementhandle, the other
		    # one has to behave exactly the same way
		    if ($sth_query->$method()->[$_] ne
			$sth_listf->$method()->[$_]) {
			$ok = 0;
			last;
		    }
		}
	    }
	    Test($state or $ok)
		or printf("Error in listfields->%s, %s <-> %s.\n",
				       $method, $sth_query->$method()->[$_],
				       $sth_listf->$method()->[$_]);
	}

	# The only difference: the ListFields sth must not have a row
	# associated with
	local($^W) = 0;
	my $got;
	if (!$state) {
	    $got = $sth_listf->numrows;
	}
	Test($state or (!defined($got) or $got == 0 or $got eq "N/A"))
	    or printf("Rows present (%s) in listfields sth.\n",
				   $sth_listf->numrows);
	Test($state or ($sth_query->numrows > 0))
	    or printf("Missing rows in sth_query.\n");

	# Please understand that features that were added later to the module
	# are tested later. Here's a very nice test. Should be easier to
	# understand than the others:
	$ok = 1;
	if (!$state) {
	    $sth_query->dataseek(0);
	    while (%hash = $sth_query->fetchhash) {
		# fetchhash stuffs the contents of the row directly into a
		# hash instead of a row. We have only two lines to check.
		# Column she has to be either 'jazz' or 'x'.
		if ($hash{she} ne 'jazz'  and  $hash{she} ne 'x') {
		    $ok = 0;
		    last;
		}
	    }
	}
	Test($state or $ok)
	    or printf("Error in fetchhash, got %s.\n",
				   $hash{she});
    }

    $query = "drop table $table1";
    Test($state or $dbh->query($query))
	or test_error($dbh, 0, $query);

    # Although it is a bad idea to specify constants in lowercase,
    # I have to test if it is supported as it has been documented:

    if ($mdriver ne 'mysql') {
	Test($state or ($class->int___type() == $class->INT_TYPE()))
	    or printf("Expected int___type to be %d, got %d.\n",
				   $class->INT_TYPE(), $class->int___type);
    }

    # Let's create another table where we inspect if we can insert
    # 8 bit characters:

    # The chr column has a size of 2 bytes, due to a bug in the
    # mSQL engine. This bug is checked for in msql1.t, so we don't
    # need to deal with it here.
    $query = "create table $table1 (ascii int, chr char(2))";
    Test($state or $dbh->query($query))
	or test_error($dbh,0,$query);

    my $nchar;
    for $nchar (1..255) {
	my $chr;
	if (!$state) {
	    $chr = $dbh->quote(chr($nchar));
	    $query = "insert into $table1 values ($nchar, $chr)";
	}
	Test($state or $dbh->query($query))
	    or ($query = unctrl($query),
			     print("Ctrl character $nchar: q[$query]"
				   . " err[$$errstrRef])\n"));
    }

    Test($state or ($sth = $dbh->query("select * from $table1")))
	or test_error($dbh);
    Test($state or ($sth->numrows() == 255))
	or print("Expected control characters to produce"
			      . " 255 rows, got $sth->numrows.\n");

    my $ok = 1;
    if (!$state) {
	while (%hash = $sth->fetchhash) {
	    if ($hash{'chr'} ne chr($hash{'ascii'})) {
		# mysql chops blanks at the right side ..
		if ($mdriver ne 'mysql'  or  $hash{'ascii'} ne 32) {
		    $ok = 0;
		    last;
		}
	    }
	}
    }

    Test($state or $ok)
	or printf("Error in control character hash at %d,"
			       . " %s <-> %s.\n", $hash{'ascii'},
			       $hash{'chr'}, chr($hash{'ascii'}));

    Test($state or ($sth = $dbh->query("drop table $table1")))
	or test_error($dbh);
    if ($mdriver eq 'mysql') {
	Test($state or ($sth->numfields == 0))
	    or printf("Expected num fields being zero, not %s.\n",
				   $sth->numfields);
    }

    # mSQL up to 1.0.16 had this annoying lost table bug, so I try to
    # force our users to upgrade somehow

    {
	my @created = ();
	local($Mysql::QUIET, $Msql::QUIET, $Msql1::QUIET) = (1, 1, 1);

	if (!$state) {
	    # create 8 tables
	    for (1..8) {
		push @created, create($dbh, $table1, q{(foo char(1))});
	    }

	    # reference all 8 so they are cached
	    for (@created) {
		$dbh->listfields($_);
	    }

	    # reference a non existant table
	    my $nonexist = "NONEXIST";
	    $nonexist++ while grep /^$nonexist$/, $dbh->listtables;
	    $dbh->listfields($nonexist);

	    # reference the first table in the cache: 1.0.16 did not know
	    # the contents
	}

	Test($state or ($dbh->listfields($created[0])->numfields != 0))
	    or ($mdriver eq 'mysql')
	    or printf STDERR ("Your version %s of the msqld has a"
			      . " serious bug,\n"
			      . "upgrade the server to something"
			      . " > 1.0.16.\n");

	if (!$state) {
	    # drop the eight tables
	    for (@created) {
		drop($dbh,$_);
	    }
	}
    }

    #
    #   Try mysql's insertid feature
    #
    if ($mdriver eq 'mysql') {
	my ($sth, $table);
	Test($state or ($table = FindNewTable($dbh)));
	Test($state or $dbh->query("CREATE TABLE $table ("
				   . " id integer AUTO_INCREMENT PRIMARY KEY,"
				   . " country char(30) NOT NULL)"))
	    or printf("Error while executing query: %s\n", $Mysql::db_errstr);
	Test($state or
	     ($sth = $dbh->query("INSERT INTO $table VALUES (NULL, 'a')")))
	    or printf("Error while executing query: %s\n", $Mysql::db_errstr);
	Test($state or
	     ($sth = $dbh->query("INSERT INTO $table VALUES (NULL, 'b')")))
	    or printf("Error while executing query: %s\n", $Mysql::db_errstr);
	Test($state or $sth->insert_id =~ /\d+/)
	    or printf("insertid generated incorrect result: %s\n",
		      $sth->insert_id);
	Test($state or $dbh->query("DROP TABLE $table"));
    }
}
