#!/my/gnu/bin/perl -w

# very slightly modified version of msql.t as in the MsqlPerl version
# 1.16

# Running the testscript with a hostname as $ARGV[0] runs the test via
# a TCP socket. Per default we connect to the unix socket to avoid
# problems you might have with resolving "localhost". Too many systems
# are configured wrong in this respect. But you're welcome to test it
# out.

# That's the standard perl way tostart a testscript. It announces that
# that many tests are to follow. And it does so before anything can go
# wrong;

BEGIN {
    do ((-f "lib.pl") ? "lib.pl" : "t/lib.pl");
    if ($mdriver ne "mysql") { print "1..0\n"; exit 0; }
    print "1..68\n";
}

my $host = shift @ARGV || $ENV{'DBI_HOST'}
  || $::test_host || $::test_host;  # Make -w happy
my $port = shift @ARGV || $ENV{'DBI_PORT'}
  || $::test_port || $::test_port;  # Make -w happy
$host .= ":$port" if $port;
my $user = shift @ARGV || $ENV{'DBI_USER'}
  || $::test_user || $::test_user;  # Make -w happy
my $password = shift @ARGV || $ENV{'DBI_PASS'}
  || $::test_password || $::test_password;  # Make -w happy
my $dbname = shift @ARGV || $ENV{'DBI_DB'}
  || $::test_db || $::test_db;  # Make -w happy

use Mysql;

# Force yourself to strict programming. See man strict for details.
# use strict;

# Variables we're going to use
my(
   $query,
   $firsttable,
   $secondtable,
   $dbh,
   $dbh2,
   $dbh3,
   $sth,
   $i,
   @row,
   %hash,
  );

# You may connect in two steps: (1) Connect and (2) SelectDB...

if ($dbh = Mysql->connect($host, $dbname, $user, $password)){
    print "ok 1\n";
} else {
    $Mysql::db_errstr ||= "";
    my $onhost = $host ? " (on $host)" : "";
    print STDERR qq{not ok 1: $Mysql::db_errstr
\tIt looks as if your server$onhost is not up and running.
\tThis test requires a running server.
\tPlease make sure your server is running and retry.
};
    exit;
}

if ($dbh->selectdb($dbname)){
    print("ok 2\n");
} else {
    die qq{not ok 2: $Mysql::db_errstr
    Please make sure that a database \"$dbname\" exists
    and that you have permission to read and write on it
};
}

# Or you may call connect with two arguments, the first being the
# host, and the second being the DB

if ($dbh = Mysql->connect($host,$dbname,$user,$password)){
    print("ok 3\n");
} else {
    die "not ok 3: $Mysql::db_errstr\n";
}

# For the error messages we're going to produce within this script we
# write a subroutine, so the typical error message will always look
# more or less similar:

sub test_error {
    my($id,$query,$error) = @_;
    $id    ||= "?";               # Newer Test::Harness will accept that
    $query ||= "";                # query is optional
    $query = "\n\tquery $query" if $query;
    $error ||= Mysql->errmsg;      # without error we ask Mysql
    print qq{Not ok $id:\n\terrmsg $error$query\n};
}


# Now we create two tables that are certainly not in the test database
# If you don't understand the trickery here, just skip this section, No big deal.
{
    my $goodtable = "TABLE00";
    my(%foundtable,@foundtable);
    @foundtable  =  $dbh->listtables;
    @foundtable{@foundtable} = (1) x @foundtable; # all existing tables are now keys in %foundtable
    my $limit = 0;

    for ($firsttable, $secondtable) {
	while () {
	    next if $foundtable{++$goodtable};
	    my $query = qq{
		create table $goodtable (
					 she char(32),
					 him char(32) not null,
					 who char (32)
					)
	    };
	    unless ($dbh->query($query)){
		die "Cannot create table: query [$query] message [$Mysql::db_errstr]\n" if $limit++ > 1000;
		next;
	    }
	    $_ = $goodtable;
	    last;
	}
    }
    # For the tests in this script we have two tablenames that we can
    # peruse: $firsttable and $secondtable
}

# Now we write some test records into the two tables. Note, we *know*,
# these tables are empty

print "Writing some test records.\n";
for $query (
	    "insert into $firsttable values ('Anna', 'Franz', 'Otto')"        ,
	    "insert into $firsttable values ('Sabine', 'Thomas', 'Pauline')"  ,
	    "insert into $firsttable values ('Jane', 'Paul', 'Jah')"	      ,
	    "insert into $secondtable values ('Henry', 'Francis', 'James')"   ,
	    "insert into $secondtable values ('Cashrel', 'Beco', 'Lotic')"
	   ) {
    $dbh->query($query) or test_error(0,$query);
}

$query = "select * from $firsttable";
$sth = $dbh->query($query) or test_error(0,$query);

($sth->numrows == 3)   and print("ok 4\n") or print("not ok 4\n"); # three rows
($sth->numfields == 3) and print("ok 5\n") or print("not ok 5\n"); # three columns

# There is the array reference $sth->name. It has to have as many
# fields as $sth->numfields tells us
print "Checking numfields.\n";
(@{$sth->name} == $sth->numfields)
    and print ("ok 6\n") or print("not ok 6\n");

# There is the array reference $sth->table. We expect, that all three
# fields in the array have the same value, as we only selected from
# $firsttable
print "Checking table.\n";
$sth->table->[0] eq $firsttable
    and print ("ok 7\n") or print("not ok 7\n");
$sth->table->[1] eq $sth->table->[2]
    and print ("ok 8\n") or print("not ok 8\n");

# CHAR_TYPE, NUM_TYPE and REAL_TYPE are exported functions from
# Mysql. That is why you have to say 'use Mysql'. The functions are
# really constants, but that's the way headerfile constants are
# handled in perl5 up to 5.001m (will probably change soon)
print "Checking type.\n";
CHAR_TYPE() == $sth->type->[0]
    and print ("ok 9\n") or print("not ok 9\n");

print "Checking number of rows.\n";
{
    # Now we count the rows ourselves, we don't trust anybody
    my $rowcnt=0;
    while (@row = $sth->fetchrow()){
	$rowcnt++;
    }

    # We haven't yet tested DataSeek, so lets count again
    $sth->dataseek(0);
    while (@row = $sth->fetchrow()){
	$rowcnt++;
    }

    # $rowcount now==6, twice the number of rows we've seen
    ($rowcnt/2 == $sth->numrows)
	and print ("ok 10\n") or print("not ok 10\n");
}


# let's see the second table
$sth = $dbh->query("select * from $secondtable") or test_error();

# We set the second field "not null". Does the API know that?
$sth->is_not_null->[1] > 0
    and print ("ok 11\n") or print("not ok 11\n");

# Are we able to just reconnect with the *same* scalar ($dbh) playing
# the role of the db-handle?
if ($dbh = Mysql->connect($host,$dbname,$user,$password)){
    print("ok 12\n");
} else {
    print "not ok 12: $Mysql::db_errstr\n";
}

# We may have an arbitrary number of statementhandles. Each
# statementhandle consumes memory, so in reality we try to scope them
# with my() within a block or we reuse them or we undef them.
{
    # Declare the statement handle as lexically scoped (see man
    # perlfunc and search for 'my EXPR') Don't forget to scope other
    # variables too, that you won't need outside the block
    my($sth1,$sth2,@row1,$count);

    $sth1 = $dbh->query("select * from $firsttable")
	or warn "Query had some problem: $Mysql::db_errstr\n";
    $sth2 = $dbh->query("select * from $secondtable")
	or warn "Query had some problem: $Mysql::db_errstr\n";

    # You have seen this above, so NO COMMENT :)
    $count=0;
    while ($sth2->fetchrow and @row1 = $sth1->fetchrow){
	$count++;
    }
    $count == 2  and print ("ok 13\n") or print("not ok 13\n");

    # When we undef this handle, the memory associated with it is
    # freed
    undef ($sth2);

    $count=0;
    while (@row1 = $sth1->fetchrow){
	$count++;
    }
    $count == 1 and print ("ok 14\n") or print("not ok 14\n");

    # When we leave this block, the memory associated with $sth1 is
    # freed
}

# What happens, when we have errors?
# Yes, there's a typo: we add a paren to the statement
{
    # The use of the -w switch is really a good idea in general, but
    # if you want the -w switch but do NOT want to see Mysql's error
    # messages, you can turn them off using $Mysql::QUIET

    local($Mysql::QUIET) = 1;
    # In reality we would say "or die ...", but in this case we forgot it:
    $sth = $dbh->query  ("select * from $firsttable
	     where him = 'Thomas')");

    # $Mysql::db_errstr should contain the word "error" now
    $dbh->errmsg =~ /error/
	and print("ok 15\n") or print("not ok 15\n");
}



# Now $sth should be undefined, because the query above failed. If we
# try to use this statementhandle, we should die. We don't want to
# die, because we are in atest script. So we check what happens with
# eval
eval "\@row = \$sth->fetchrow;";
if ($@){print "ok 16\n"} else {print "not ok 16\n"}


# Remember, we inserted a row into table $firsttable ('Sabine',
# 'Thomas', 'Pauline'). Let's see, if they are still there.
$sth = $dbh->query  ("select * from $firsttable
     where him = 'Thomas'")
     or warn "query had some problem: $Mysql::db_errstr\n";

@row = $sth->fetchrow or warn "$firsttable didn't find a matching row";
$row[2] eq "Pauline" and print ("ok 17\n") or print("not ok 17\n");

{
    # %fieldnum is a hash that associates the index number for each field
    # name:
    my %fieldnum;
    @fieldnum{@{$sth->name}} = 0..@{$sth->name}-1;
    
    # %fieldnum is now (she => 0, him => 1, who => 2)
    
    # So we do not have to hard-code the zero for "she" here
    $row[$fieldnum{"she"}] eq 'Sabine'
	and print ("ok 18\n") or print("not ok 18\n");
}

# After 18 tests, the database handle may feel the desire to rest. Or
# maybe the writer of this script has forgotten, that he is already
# connected

# While in reality you should use your database connections
# economically -- they cost you a slot in the server connection table,
# and you can easily run out of available slots -- we, in the test
# script want to know what happens with more than one handle
if ($dbh2 = Mysql->connect($host,$dbname,$user,$password)){
    print("ok 19\n");
} else {
    print "not ok 19\n";
}

# Some quick checks about the contents of the handle...
$dbh2->database eq $dbname and print("ok 20\n") or print("not ok 20\n");
$dbh2->sock =~ /^\d+$/ and print("ok 21\n") or print("not ok 21\n");

# Is $dbh2 able to drop a table, while we are connected with $dbh?
# Sure it can...
$dbh2->query("drop table $secondtable") and print("ok 22\n") or print("not ok 22\n");


{
    # Does ListDBs find the test database? Sure...
    my @array = $dbh2->listdbs;
    grep( /^$dbname$/, @array ) and print("ok 23\n") or print("not ok 23\n");

    # Does ListTables now find our $firsttable?
    @array = $dbh2->listtables;
    grep( /^`$firsttable`$/i, @array )  and print("ok 24\n") or print("not ok 24\n");
}

# The third connection within a single script. I promise, this will do...
if ($dbh3 = Connect Mysql($host,$dbname,$user,$password)){
    print("ok 25\n");
} else {
    test_error(25,"connect->$host");
}

$dbh3->host eq $host and print("ok 26\n") or print "not ok 26\n";
$dbh3->database eq $dbname and print("ok 27\n") or print "not ok 27\n";


# For what it's worth, we have a tough job for the server here. First
# we define two simple subroutines. The goal of these is to make the
# create table statement independent of what happens on the server
# side. If the table cannot be created we magic increment the
# suggested name and retry. We return the incremented table name. With
# this setting we can run the test script in parallel in many
# processes.

sub create {
    my($db,$tablename,$createexpression) = @_;
    my($query) = "create table $tablename $createexpression";
    my $limit = 0;
    while (! $db->query($query)){
	die "Cannot create table: query [$query] message [$Mysql::db_errstr]\n" if $limit++ > 1000;
	$tablename++;
	$query = "create table $tablename $createexpression";
    }
    $tablename;
}

sub drop { shift->query("drop table $_[0]"); }

# Then we insert some nonsense changing the dbhandle quickly
{
    my $C="AAAA"; 
    my $N=1;
    drop($dbh2,$firsttable);
    $firsttable = create($dbh2,$firsttable,"( name char(40) not null,
            num int, country char(4), mytime real )");

    for (1..5){
	$dbh2->query("insert into $firsttable values
	('".$C++."',".$N++.",'".$C++."',".rand().")") or test_error();
	$dbh3->query("insert into $firsttable values
	('".$C++."',".$N++.",'".$C++."',".rand().")") or test_error();
    }
}

# I haven't shown you yet a cute (and dirty) trick to save memory. As
# ->query returns an object you can reference this object in a single
# chain of -> operators. The statement handle is not preserved, and
# the memory associated with it is cleaned up within a single
# statement. 'Course you never know, which part of the statement
# failed--if something fails.

$dbh2->query("select * from $firsttable")->numrows == 10
    and print("ok 28\n") or print("not ok 28\n");

# Interesting the following test. Creating and dropping of tables via
# two different database handles in quick alteration. There was really
# a version of Mysql that messed up with this

for (1..3){
    drop($dbh2,$firsttable);
    $secondtable = create($dbh3,$secondtable,"( name char(40) not null,
            num int, country char(4), mytime real )");
    drop($dbh2,$secondtable);
    $firsttable = create($dbh3,$firsttable,"( name char(40) not null,
            num int, country char(4), mytime real )");
}
drop($dbh2,$firsttable) and  print("ok 29\n") or print("not ok 29\n");

# A quick check, if the array @{$sth->length} is available and
# correct. See man perlref for an explanation of this kind of
# referencing/dereferencing. Watch out, that we still use an old
# statement handle here. The corresponding table has been overwritten
# quite a few times, but as we are dealing with an in-memeory copy, we
# still have it available

if ("@{$sth->length}" eq "32 32 32"){
    print "ok 30\n";
} else {
    print "not ok 30\n";
}


# Here were two useless tests a while back that didn't please me after
# a while

print "ok 31\n";
print "ok 32\n";

# The following tests show, that NULL fields (introduced with
# Mysql-1.0.6) are handled correctly:

if ($dbh->getserverinfo lt 2) { # Before version 2 we have the "primary key" syntax
    $firsttable = create($dbh,$firsttable,"( she char(14) primary key not null,
	him int, who char(1))") or test_error();
} else {
    $firsttable = create($dbh,$firsttable,"( she char(14) not null,
	him int, who char(1))") or test_error();
    $dbh->query("create unique index she_index on $firsttable ( she )") or test_error();
}

# As you see, we don't insert a value for "him" and "who", so we can
# test the undefinedness

$dbh->query("insert into $firsttable (she) values ('jazz')") or test_error;

$sth = $dbh->query("select * from $firsttable") or test_error;
@row = $sth->fetchrow() or test_error;

# "she" is "jazz", thusly defined

if (defined $row[0]) {
    print "ok 33\n";
} else {
    print "not ok 33\n";
}

# field "him", a character field, should not be defined

if (defined $row[1]) {
    print "not ok 34\n";
} else {
    print "ok 34\n";
}

# field "who", an integer field, should not be defined

if (defined $row[2]) {
    print "not ok 35\n";
} else {
    print "ok 35\n";
}

# So far we have evaluated metadata in scalar context. Let's see,
# if array context works

$i = 35;
foreach (qw/table name type is_not_null is_pri_key length/) {
    my @arr = $sth->$_();
    if (@arr == 3){
	print "ok ", ++$i, "\n";
    } else {
	print "not ok ", ++$i, ": @arr\n";
    }
}
    
# mSQL: A non-select should return TRUE, and if anybody tries to use this
# mSQL: return value as an object reference, we should not core dump
# In mysql a query always return an object!

{
    local($Mysql::QUIET) = 1;
    $sth = $dbh->query("insert into $firsttable values (\047x\047,2,\047y\047)");
    if (!defined($sth->fetchrow))
    {
	print "ok 42\n";
    }
}
    

{
    my($sth_query,$sth_listf,$method);

    # So many people have problems using the ListFields method,
    # so we finally provide a simple example.
    $sth_query = $dbh->query("select * from $firsttable");
    $sth_listf = $dbh->listfields($firsttable);
    $i = 43;
    for $method (qw/name table length type is_not_null is_pri_key/) {
	for (0..$sth_query->numfields -1) {
	    # whatever we do to the one statementhandle, the other one has
	    # to behave exactly the same way
	    if ($sth_query->$method()->[$_] eq $sth_listf->$method()->[$_]) {
		print "ok $i\n" ;
	    } else {
		print "not ok $i\n";
	    }
	    $i++;
	}
    }
    
    # The only difference: the ListFields sth must not have a row associated with
    local($^W) = 0;
    my($got) = $sth_listf->numrows;
    if (!defined $got or $got == 0) {
	print "ok 61\n";
    } else {
	print "not ok 61 - got [$got]\n";
    }
    if ($sth_query->numrows > 0) {
	print "ok 62\n";
    } else {
	print "not ok 62\n";
    }
    
    # Please understand that features that were added later to the module
    # are tested later. Here's a very nice test. Should be easier to
    # understand than the others:
    
    $sth_query->dataseek(0);
    $i = 63;
    while (%hash = $sth_query->fetchhash) {
	
	# fetchhash stuffs the contents of the row directly into a hash
	# instead of a row. We have only two lines to check. Column she
	# has to be either 'jazz' or 'x'.
	if ($hash{she} eq 'jazz' or $hash{she} eq 'x') {
	    print "ok $i\n";
	} else {
	    print "not ok $i\n";
	}
	$i++;
    }
}    

$dbh->query("drop table $firsttable") or test_error;

# Although it is a bad idea to specify constants in lowercase,
# I have to test if it is supported as it has been documented:

if (Mysql::int___type() == INT_TYPE) {
    print "ok 65\n";
} else {
    print "not ok 65\n";
}


# Let's create another table where we inspect if we can insert
# 8 bit characters:

# For mysql, changed character to charactr and char(1) to blob

$query = "create table $secondtable (ascii int, charactr blob)";
$dbh->query($query) or test_error;
my $nchar;
my $not_ok;
for $nchar (1..255) {
    my $chr = $dbh->quote(chr($nchar));
    $query = qq{
insert into $secondtable values ($nchar, $chr)
    };
    unless ($dbh->query($query)) {
	$query = unctrl($query);
	$not_ok .= "\t(q[$query] err[$Mysql::db_errstr])\n";
    }
}

sub unctrl {
    my $str = shift;
    $str =~ s/([\000-\037\177])/ '^' . pack('c', ord($1) ^ 64) /eg;
    return $str;
}

$sth = $dbh->query("select * from $secondtable") or test_error;
if ($sth->numrows() == 255){
    print "ok 66\n";
} else {
    print "not ok 66 #" . $not_ok;
}
while (%hash = $sth->fetchhash) {
    $hash{charactr} eq chr($hash{ascii}) or print "not ok 67 [char no $hash{ascii}]\n";
}
print "ok 67\n";

$dbh->query("drop table $secondtable") or test_error;

# mSQL up to 1.0.16 had this annoying lost table bug, so I try to
# force our users to upgrade to 1.0.17

{
    my @created = ();
    local($Mysql::QUIET) = 1;

    # create 8 tables
    for (1..8) {
	push @created, create($dbh,$firsttable,q{(foo char(1))});
    }

    # reference all 8 so they are cached
    for (@created) {
	$dbh->listfields($_);
    }

    # reference a non existant table
    my $nonexist = "NONEXIST";
    $nonexist++ while grep /^$nonexist$/, $dbh->listtables;
    $dbh->listfields($nonexist);

    # reference the first table in the cache: 1.0.16 did not know the contents
    if ( $dbh->listfields($created[0])->numfields == 0) {
	my $version = $dbh->getserverinfo;
	print "not ok 68\n";
        print STDERR "Your version $version of the mSQL has a serious bug,
\teither upgrade the server to > 1.0.16 or read the file patch.lost.tables\n";
    } else {
	print "ok 68\n";
    }

    # drop the eight tables
    for (@created) {
	drop($dbh,$_);
    }
}

