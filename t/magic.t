use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
require "t/lib.pl";

my $tb = Test::More->builder;
binmode $tb->failure_output, ":utf8";
binmode $tb->todo_output,    ":utf8";

my $dbh = eval { DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, AutoCommit => 0, mysql_server_prepare_disable_fallback => 1 }) };
plan skip_all => "no database connection" if $@ or not $dbh;

plan tests => 288*2;

$dbh->do("CREATE TEMPORARY TABLE t(i INT)");

foreach my $mysql_enable_utf8 (0, 1) {
    $dbh->{mysql_enable_utf8} = $mysql_enable_utf8;
    foreach my $mysql_server_prepare (0, 1) {
        $dbh->{mysql_server_prepare} = $mysql_server_prepare;
        foreach my $val ('1', 1, 1.0, 1.1, undef, 'XX', "\N{U+100}") {
            next if defined $val and (my $tmp1 = $val) eq "\N{U+100}" and not $mysql_enable_utf8;
            {
                my $param_str = $val;
                tie my $param, 'TieScalarCounter', $param_str;
                my $statement_str = "SELECT * FROM t WHERE i = " . $dbh->quote($param_str) . " OR i = ?";
                tie my $statement, 'TieScalarCounter', $statement_str;
                my $func = '$dbh->do(' . $statement_str . ', {}, ' . (defined $param_str ? $param_str : 'undef')  . ')';
                $dbh->do($statement, {}, $param);
                is(tied($statement)->{fetch}, 1, "$func processes get magic on statement only once");
                is(tied($statement)->{store}, 0, "$func does not process set magic on statement");
                is(tied($param)->{fetch}, 1, "$func processes get magic on param only once");
                is(tied($param)->{store}, 0, "$func does not process set magic on param");
            }
            {
                my $param_str = $val;
                tie my $param, 'TieScalarCounter', $param_str;
                my $statement_str = "SELECT * FROM t WHERE i = " . $dbh->quote($param_str) . " OR i = ?";
                tie my $statement, 'TieScalarCounter', $statement_str;
                my $func = '$dbh->selectall_arrayref(' . $statement_str . ', {}, ' . (defined $param_str ? $param_str : 'undef')  . ')';
                $dbh->selectall_arrayref($statement, {}, $param);
                is(tied($statement)->{fetch}, 1, "$func processes get magic on statement only once");
                is(tied($statement)->{store}, 0, "$func does not process set magic on statement");
                is(tied($param)->{fetch}, 1, "$func processes get magic on param only once");
                is(tied($param)->{store}, 0, "$func does not process set magic on param");
            }
            {
                my $param_str = $val;
                tie my $param, 'TieScalarCounter', $param_str;
                my $statement_str = "SELECT * FROM t WHERE i = " . $dbh->quote($param_str) . " OR i = ?";
                tie my $statement, 'TieScalarCounter', $statement_str;
                my $func1 = '$dbh->prepare(' . $statement_str . ')';
                my $func2 = '$sth->execute(' . (defined $param_str ? $param_str : 'undef')  . ')';
                my $sth = $dbh->prepare($statement);
                $sth->execute($param);
                $sth->finish();
                is(tied($statement)->{fetch}, 1, "$func1 processes get magic on statement only once");
                is(tied($statement)->{store}, 0, "$func1 does not process set magic on statement");
                is(tied($param)->{fetch}, 1, "$func2 processes get magic on param only once");
                is(tied($param)->{store}, 0, "$func2 does not process set magic on param");
            }
            {
                my $param_str = $val;
                tie my $param, 'TieScalarCounter', $param_str;
                my $statement_str = "SELECT * FROM t WHERE i = " . $dbh->quote($param_str) . " OR i = ?";
                tie my $statement, 'TieScalarCounter', $statement_str;
                my $func1 = '$dbh->prepare(' . $statement_str . ')';
                my $func2 = '$sth->bind_param(1, ' . (defined $param_str ? $param_str : 'undef')  . ')';
                my $sth = $dbh->prepare($statement);
                $sth->bind_param(1, $param);
                $sth->execute();
                $sth->finish();
                is(tied($statement)->{fetch}, 1, "$func1 processes get magic on statement only once");
                is(tied($statement)->{store}, 0, "$func1 does not process set magic on statement");
                is(tied($param)->{fetch}, 1, "$func2 processes get magic on param only once");
                is(tied($param)->{store}, 0, "$func2 does not process set magic on param");
            }
            next if defined $val and (my $tmp2 = $val) !~ /^[\d.]+$/;
            {
                my $param_str = $val;
                tie my $param, 'TieScalarCounter', $param_str;
                my $statement_str = "SELECT * FROM t WHERE i = " . $dbh->quote($param_str) . " OR i = ?";
                tie my $statement, 'TieScalarCounter', $statement_str;
                my $func1 = '$dbh->prepare(' . $statement_str . ')';
                my $func2 = '$sth->bind_param(1, ' . (defined $param_str ? $param_str : 'undef')  . ', DBI::SQL_INTEGER)';
                my $sth = $dbh->prepare($statement);
                $sth->bind_param(1, $param, DBI::SQL_INTEGER);
                $sth->execute();
                $sth->finish();
                is(tied($statement)->{fetch}, 1, "$func1 processes get magic on statement only once");
                is(tied($statement)->{store}, 0, "$func1 does not process set magic on statement");
                is(tied($param)->{fetch}, 1, "$func2 processes get magic on param only once");
                is(tied($param)->{store}, 0, "$func2 does not process set magic on param");
            }
            {
                my $param_str = $val;
                tie my $param, 'TieScalarCounter', $param_str;
                my $statement_str = "SELECT * FROM t WHERE i = " . $dbh->quote($param_str) . " OR i = ?";
                tie my $statement, 'TieScalarCounter', $statement_str;
                my $func1 = '$dbh->prepare(' . $statement_str . ')';
                my $func2 = '$sth->bind_param(1, ' . (defined $param_str ? $param_str : 'undef')  . ', DBI::SQL_FLOAT)';
                my $sth = $dbh->prepare($statement);
                $sth->bind_param(1, $param, DBI::SQL_FLOAT);
                $sth->execute();
                $sth->finish();
                is(tied($statement)->{fetch}, 1, "$func1 processes get magic on statement only once");
                is(tied($statement)->{store}, 0, "$func1 does not process set magic on statement");
                is(tied($param)->{fetch}, 1, "$func2 processes get magic on param only once");
                is(tied($param)->{store}, 0, "$func2 does not process set magic on param");
            }
        }
    }
}

$dbh->disconnect();

package TieScalarCounter;

sub TIESCALAR {
    my ($class, $value) = @_;
    return bless { fetch => 0, store => 0, value => $value }, $class;
}

sub FETCH {
    my ($self) = @_;
    $self->{fetch}++;
    return $self->{value};
}

sub STORE {
    my ($self, $value) = @_;
    $self->{store}++;
    $self->{value} = $value;
}
