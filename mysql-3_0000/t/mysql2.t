#!/my/gnu/bin/perl -w

######################### We start with some black magic to print on failure.

use strict;
use vars qw($loaded $mdriver);

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)

$| = 1;
do ((-f "lib.pl") ? "lib.pl" :
    (-f "t/lib.pl" ? "t/lib.pl" : "Mysql/t/lib.pl"));

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

if ($mdriver ne "mysql") { print "1..0\n"; exit 0; }
eval { require Mysql };
my $db = Mysql->connect($host, $dbname, $user, $password);
if ($db->getserverinfo lt 2) {
    print "1..0\n";
    exit;
}
print "1..37\n";
END {print "not ok 1\n" unless $loaded;}

######################### End of black magic.

$loaded = 1;
print "ok 1\n";

{
    my($q,$what,@t,$i,$j);
    my $db = Mysql->connect($host,$dbname,$user,$password);
    $t[0] = create(
		   $db,
		   "TABLE00",
		   "( id char(4) not null, longish tinyblob )");
    $t[1] = create(
		   $db,
		   "TABLE01",
		   "( id char(4) not null, longish blob )");
    if (grep /^`$t[0]`$/i, $db->listtables) {
	print "ok 2\n";
    } else {
	print "not ok 2\n";
    }
    for $i (0..14) {
	for $j (0,1) {
	    $q = qq{insert into $t[$j] values \('00$i',\'}.bytometer(2**$i).qq{\'\)};
	    my $ok = 3 + $i*2 + $j;
	    print "Query: $q\n";
	    if ($db->query($q)->affected_rows == 1) {
		print "ok $ok\n";
	    } else {
		print "not ok $ok\n";
	    }
	}
    }
    $q = qq{select * from $t[0] where id < '006' and id > '002' order by id};
    if (($what = $db->query($q)->numrows) == 3) {
	print "ok 33\n";
    } else {
	print "not ok 33: $what\n";
    }
    $q = qq{select $t[0].id from $t[0] where id < '006' and id > '002' order by id desc};
    if (($what = $db->query($q)->numrows) == 3) {
	print "ok 34\n";
    } else {
	print "not ok 34: $what\n";
    }
    $q = qq{select * from $t[0] where id like '[_]'  order by id};
    if ($db->query($q)->numrows==0) {
	print "ok 35\n";
    } else {
	print "not ok 35: $what\n";
    }
    my $index = cre_index($db,'INDEX00',"on $t[1] (id)","unique");
    print $index ? "" : "not ", "ok 36\n";

    $q = qq{select $t[0].id, $t[1].id from $t[0], $t[1] where $t[0].id=$t[1].id};
    if ($db->query($q)->numrows==15) {
	print "ok 37\n";
    } else {
	print "not ok 37: $what\n";
    }

    $q = qq{drop table $t[0]};
    $db->query($q);
    $q = qq{drop table $t[1]};
    $db->query($q);
}

sub create {
    my($db,$tablename,$createexpression) = @_;
    my($query) = "create table $tablename $createexpression";
    local($Mysql::QUIET) = 1;
    my $limit = 0;
    while (! $db->query($query)){
	die "Cannot create table: query [$query] message [$Mysql::db_errstr]\n" if $limit++ > 1000;
	$tablename++;
	$query = "create table $tablename $createexpression";
    }
    $tablename;
}

sub cre_index {
    my($db,$indexname,$createexpression,$uniq) = @_;
    my($query) = "create $uniq index $indexname $createexpression";
    local($Mysql::QUIET) = 1;
    my $limit = 0;
    while (! $db->query($query)){
	die "Cannot create index: query [$query] message [$Mysql::db_errstr]\n" if $limit++ > 1000;
	$indexname++;
	$query = "create $uniq index $indexname $createexpression";
    }
    $indexname;
}

sub bytometer {
    my($byte) = @_;
    my($result,$i) = "";
    for ($i=5;$i<=$byte;$i+=5) {
	if ( $i==5 || substr($i,-2) eq "05" && $i<10000 ) {
	    $result .=  join "", "\n", "." x (4-length($i)), $i;
	} elsif ( $i<=10000 ) {
	    $result .=  join "", "." x (5-length($i)), $i;
	} elsif ( substr($i,-2) eq "10" ) {
	    $result .=  join "", "\n", "." x (9-length($i)), $i;
	} elsif ( substr($i,-1) eq "0" ) {
	    $result .=  join "", "." x (10-length($i)), $i;
	}
    }
    $result .= "." x ($byte%5);
    return $result;
}
