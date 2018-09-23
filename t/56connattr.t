#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use DBI::Const::GetInfoType;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password $table);

my $dbh;
eval { $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1,
                        PrintError => 0,
                        AutoCommit => 0,
                        mysql_conn_attrs => { foo => 'bar' },
                        }
                        );
     };
if ($@) {
    plan skip_all => "no database connection";
}

my @pfenabled = $dbh->selectrow_array("show variables like 'performance_schema'");
if (!@pfenabled) {
  plan skip_all => 'performance schema not available';
}
if ($pfenabled[1] ne 'ON') {
  plan skip_all => 'performance schema not enabled';
}

if ($dbh->{mysql_clientversion} < 50606) {
  plan skip_all => 'client version should be 5.6.6 or later';
}

eval {$dbh->do("select * from performance_schema.session_connect_attrs where processlist_id=connection_id()");};
if ($@) {
  $dbh->disconnect();
  plan skip_all => "no permission on performance_schema tables";
}

plan tests => 8;

my $rows = $dbh->selectall_hashref("select * from performance_schema.session_connect_attrs where processlist_id=connection_id()", "ATTR_NAME");

my $pid =$rows->{_pid}->{ATTR_VALUE};
cmp_ok $pid, '==', $$;

my $progname =$rows->{program_name}->{ATTR_VALUE};
cmp_ok $progname, 'eq', $0;

my $foo_attr =$rows->{foo}->{ATTR_VALUE};
cmp_ok $foo_attr, 'eq', 'bar';

for my $key ('_platform','_client_name','_client_version','_os') {
  my $row = $rows->{$key};

  cmp_ok defined $row, '==', 1, "attribute $key";
}

ok $dbh->disconnect;
