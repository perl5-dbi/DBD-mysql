#!/usr/bin/perl
use strict;
use warnings;

use DBI;
use Test::More;
use Data::Dumper;
use English qw( -no_match_vars );
our $VERSION = 0.01;

my $CONF =  $ENV{MYCONF} || "$ENV{HOME}/.my.cnf";
my $emulate = 0;
my $dbh;
eval {
    #
    # change the connection statements
    # to suit your purposes
    $dbh =

DBI->connect("dbi:mysql:test",
        'root',
        '',
        {RaiseError => 1, PrintError => 1} )
        or die "can't connect : $DBI::errstr\n";

};
if ($EVAL_ERROR) {
     plan (skip_all => " -- no connection available $EVAL_ERROR");
}
else {
    plan ( tests => 10 );
}

print "\nEmulation of ps: $emulate, version: $DBD::mysql::VERSION\n";

print $dbh->{mysql_server_prepare},"\n"; 

my $drop_proc = qq{
    drop procedure if exists test_multi_sets
};
my $create_proc = qq{
    create procedure test_multi_sets ()
    deterministic
    begin
        select user() as first_col;
        select user() as first_col, now() as second_col;
        select user() as first_col, now() as second_col, now() as third_col;
    end
};

eval { $dbh->do($drop_proc) };
ok( ! $EVAL_ERROR, 'drop procedure' );

eval { $dbh->do($create_proc) };
ok( ! $EVAL_ERROR , 'create procedure');



my $sth;

print $dbh->{mysql_server_prepare},"\n";

eval { $sth = $dbh->prepare(qq{call test_multi_sets() }) } ;
ok( $sth , 'preparing statement handler');




eval { $sth->execute() };
ok( ! $EVAL_ERROR, 'executing sth - 1st time ' );
diag $EVAL_ERROR if $EVAL_ERROR;

my $dataset;

eval { $dataset = $sth->fetchrow_arrayref();  } ;
print Dumper($dataset),"\n";
ok( $dataset && @$dataset == 1 , 'fetching first dataset');

my $more_results;

eval { $more_results =  $sth->more_results() };
ok( $more_results, 'more results available (1st time) ' ) ;

eval { $dataset = $sth->fetchrow_arrayref();  } ;
print Dumper($dataset),"\n";

ok( $dataset && @$dataset == 2 , 'fetching second dataset');

eval { $more_results =  $sth->more_results() };
ok( $more_results, 'more results available (2nd time) ' ) ;

eval { $dataset = $sth->fetchrow_arrayref();  } ;
print Dumper($dataset),"\n";

ok( $dataset && @$dataset == 3 , 'fetching third dataset');

eval { $more_results =  $sth->more_results() };
ok( ! $more_results, 'no more results available' ) ;

