use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $COUNT_CONNECT = 4000;   # Number of connect/disconnect iterations

my $have_storable;

if (!$ENV{EXTENDED_TESTING}) {
    plan skip_all => "\$ENV{EXTENDED_TESTING} is not set\n";
}

eval { require Proc::ProcessTable; };
if ($@) {
    plan skip_all => "module Proc::ProcessTable not installed \n";
}

eval { require Storable };
$have_storable = $@ ? 0 : 1;

plan tests => 3;

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


my ($size, $prev_size, $ok, $not_ok, $dbh2);
note "Testing memory leaks in connect/disconnect\n";

$ok = 0;
$not_ok = 0;
$prev_size= undef;

# run reconnect with a bad password
for (my $i = 0;  $i < $COUNT_CONNECT;  $i++) {
    eval { $dbh2 = DBI->connect($test_dsn, $test_user, "$test_password ",
                               { RaiseError => 1, 
                                 PrintError => 1,
                                 AutoCommit => 0 });};

    if ($i % 100  ==  99) {
        $size = size();
        if (defined($prev_size)) {
            if ($size == $prev_size) {
                $ok++;
            }
            else {
                diag "$prev_size => $size" if $ENV{TEST_VERBOSE};
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

ok $ok, "\$ok $ok";
ok !$not_ok, "\$not_ok $not_ok";
cmp_ok $ok, '>', $not_ok, "\$ok $ok \$not_ok $not_ok";
