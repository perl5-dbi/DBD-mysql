#!perl -w
#
#   $Id$
#
#   This is a skeleton test. For writing new tests, take this file
#   and modify/extend it.
#
use strict;
use DBI;
use Test::More;
use Carp qw(croak);
use Data::Dumper;
use vars qw($table $test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $COUNT_CONNECT = 4000;   # Number of connect/disconnect iterations
my $COUNT_PREPARE = 10000;  # Number of prepare/execute/finish iterations

my $have_storable;

if (!$ENV{SLOW_TESTS}) {
    plan skip_all => "Skip \$ENV{SLOW_TESTS} is not set\n";
}

eval { require Proc::ProcessTable; };
if ($@) {
    plan skip_all => "Skip Proc::ProcessTable not installed \n";
}

eval { require Storable };
$have_storable = $@ ? 0 : 1;

my ($dbh, $sth);
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => 
        "ERROR: $@. Can't continue test";
}
plan tests => 21; 

sub size {
  my($p, $pt);
  $pt = Proc::ProcessTable->new('cache_ttys' => $have_storable);
  for $p (@{$pt->table()}) {
    if ($p->pid() == $$) {
      return $p->size();
    }
  }
  die "Cannot find my own process?!?\n";
  exit 0;
}

ok $dbh->do("DROP TABLE IF EXISTS $table");

my $create= <<EOT;
CREATE TABLE $table (
  id INT(4) NOT NULL DEFAULT 0,
  name VARCHAR(64) NOT NULL DEFAULT ''
  )
EOT

ok $dbh->do($create);

my ($size, $prev_size, $ok, $not_ok, $dbh2, $msg);
print "Testing memory leaks in connect/disconnect\n";
$msg = "Possible memory leak in connect/disconnect detected";

$ok = 0;
$not_ok = 0;
$prev_size= undef;

for (my $i = 0;  $i < $COUNT_CONNECT;  $i++) {
  eval {$dbh2 = DBI->connect($test_dsn, $test_user, $test_password,
    { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
  if ($@) {
    $not_ok++;
    last;
  }

	if ($i % 100  ==  99) {
    $size = size();
    if (defined($prev_size)) {
      if ($size == $prev_size) {
        $ok++;
      }
      else {
        $not_ok++;
      }
    }
    else {
        $prev_size = $size;
        $size      = size();
    }
		$prev_size = $size;
  }
}
$dbh2->disconnect;

ok $ok, "\$ok $ok";
ok !$not_ok, "\$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

print "Testing memory leaks in prepare/execute/finish\n";
$msg = "Possible memory leak in prepare/execute/finish detected";

$ok = 0;
$not_ok = 0;
undef $prev_size;

for (my $i = 0;  $i < $COUNT_PREPARE;  $i++) {
  my $sth = $dbh->prepare("SELECT * FROM $table");
  $sth->execute();
  $sth->finish();

  if ($i % 100  ==  99) {
    $size = size();
    if (defined($prev_size))
    {
      if ($size == $prev_size) { 
        $ok++;
      }
      else {
        $not_ok++;
      }
    }
    else {
        $prev_size = $size;
        $size      = size();
    }
    $prev_size = $size;
  }
}

ok $ok;
ok !$not_ok, "\$ok $ok \$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

print "Testing memory leaks in fetchrow_arrayref\n";
$msg= "Possible memory leak in fetchrow_arrayref detected";

$sth= $dbh->prepare("INSERT INTO $table VALUES (?, ?)") ;

my $dataref= [[1, 'Jochen Wiedmann'],
  [2, 'Andreas K�nig'],
  [3, 'Tim Bunce'],
  [4, 'Alligator Descartes'],
  [5, 'Jonathan Leffler']];

for (@$dataref) {
  ok $sth->execute($_->[0], $_->[1]),
    "insert into $table values ($_->[0], '$_->[1]')";
}

$ok = 0;
$not_ok = 0;
undef $prev_size;

for (my $i = 0;  $i < $COUNT_PREPARE;  $i++) {
  {
    my $sth = $dbh->prepare("SELECT * FROM $table");
    $sth->execute();
    my $row;
    while ($row = $sth->fetchrow_arrayref()) { }
    $sth->finish();
  }

  if ($i % 100  ==  99) {
    $size = size();
    if (defined($prev_size)) {
      if ($size == $prev_size) {
        ++$ok;
      }
      else {
        ++$not_ok;
      }
    }
    else {
        $prev_size = $size;
        $size      = size();
    }
    $prev_size = $size;
  }
}

ok $ok;
ok !$not_ok, "\$ok $ok \$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

print "Testing memory leaks in fetchrow_hashref\n";
$msg = "Possible memory leak in fetchrow_hashref detected";

$ok = 0;
$not_ok = 0;
undef $prev_size;

for (my $i = 0;  $i < $COUNT_PREPARE;  $i++) {
  {
    my $sth = $dbh->prepare("SELECT * FROM $table");
    $sth->execute();
    my $row;
    while ($row = $sth->fetchrow_hashref()) { }
    $sth->finish();
  }

  if ($i % 100  ==  99) {
    $size = size();
    if (defined($prev_size)) {
      if ($size == $prev_size) {
        ++$ok;
      }
      else {
        ++$not_ok;
      }
    }
    else {
        $prev_size = $size;
        $size      = size();
    }
    $prev_size = $size;
  }
}

ok $ok;
ok !$not_ok, "\$ok $ok \$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";

ok $dbh->do("DROP TABLE $table");
ok $dbh->disconnect;
