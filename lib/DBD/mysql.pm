#   -*- cperl -*-

package DBD::mysql;
use strict;
use vars qw(@ISA $VERSION $err $errstr $drh);

use DBI ();
use DynaLoader();
use Carp ();
@ISA = qw(DynaLoader);

$VERSION = '2.1027_2';

bootstrap DBD::mysql $VERSION;


$err = 0;	# holds error code   for DBI::err
$errstr = "";	# holds error string for DBI::errstr
$drh = undef;	# holds driver handle once initialised

sub driver{
    return $drh if $drh;
    my($class, $attr) = @_;

    $class .= "::dr";

    # not a 'my' since we use it above to prevent multiple drivers
    $drh = DBI::_new_drh($class, { 'Name' => 'mysql',
				   'Version' => $VERSION,
				   'Err'    => \$DBD::mysql::err,
				   'Errstr' => \$DBD::mysql::errstr,
				   'Attribution' => 'DBD::mysql by Jochen Wiedmann'
				 });

    $drh;
}

sub CLONE {
  undef $drh;
}

sub _OdbcParse($$$) {
    my($class, $dsn, $hash, $args) = @_;
    my($var, $val);
    if (!defined($dsn)) {
	return;
    }
    while (length($dsn)) {
	if ($dsn =~ /([^:;]*)[:;](.*)/) {
	    $val = $1;
	    $dsn = $2;
	} else {
	    $val = $dsn;
	    $dsn = '';
	}
	if ($val =~ /([^=]*)=(.*)/) {
	    $var = $1;
	    $val = $2;
	    if ($var eq 'hostname'  ||  $var eq 'host') {
		$hash->{'host'} = $val;
	    } elsif ($var eq 'db'  ||  $var eq 'dbname') {
		$hash->{'database'} = $val;
	    } else {
		$hash->{$var} = $val;
	    }
	} else {
	    foreach $var (@$args) {
		if (!defined($hash->{$var})) {
		    $hash->{$var} = $val;
		    last;
		}
	    }
	}
    }
}

sub _OdbcParseHost ($$) {
    my($class, $dsn) = @_;
    my($hash) = {};
    $class->_OdbcParse($dsn, $hash, ['host', 'port']);
    ($hash->{'host'}, $hash->{'port'});
}

sub AUTOLOAD {
    my ($meth) = $DBD::mysql::AUTOLOAD;
    my ($smeth) = $meth;
    $smeth =~ s/(.*)\:\://;

    my $val = constant($smeth, @_ ? $_[0] : 0);
    if ($! == 0) { eval "sub $meth { $val }"; return $val; }

    Carp::croak "$meth: Not defined";
}

1;


package DBD::mysql::dr; # ====== DRIVER ======
use strict;

sub connect {
    my($drh, $dsn, $username, $password, $attrhash) = @_;
    my($port);
    my($cWarn);

    # Avoid warnings for undefined values
    $username ||= '';
    $password ||= '';

    # create a 'blank' dbh
    my($this, $privateAttrHash);
    $privateAttrHash = {
	'Name' => $dsn,
	'user' => $username,
	'password' => $password
    };

    DBD::mysql->_OdbcParse($dsn, $privateAttrHash,
				    ['database', 'host', 'port']);

    if (!defined($this = DBI::_new_dbh($drh, {'Name' => $dsn},
				       $privateAttrHash))) {
	return undef;
    }

    # Call msqlConnect func in mSQL.xs file
    # and populate internal handle data.
    DBD::mysql::db::_login($this, $dsn, $username, $password)
	  or $this = undef;

    if ($this && ($ENV{MOD_PERL} || $ENV{GATEWAY_INTERFACE})) {
        $this->{mysql_auto_reconnect} = 1;
    }
    $this;
}

sub data_sources {
    my($self) = shift;
    my($attributes) = shift;
    my($host, $port) = ('', '');
    if ($attributes) {
      $host = $attributes->{host} || '';
      $port = $attributes->{port} || '';
    }
    my(@dsn) = $self->func($host, $port, '_ListDBs');
    my($i);
    for ($i = 0;  $i < @dsn;  $i++) {
	$dsn[$i] = "DBI:mysql:$dsn[$i]";
    }
    @dsn;
}

sub admin {
    my($drh) = shift;
    my($command) = shift;
    my($dbname) = ($command eq 'createdb'  ||  $command eq 'dropdb') ?
	shift : '';
    my($host, $port) = DBD::mysql->_OdbcParseHost(shift(@_) || '');
    my($user) = shift || '';
    my($password) = shift || '';

    $drh->func(undef, $command,
	       $dbname || '',
	       $host || '',
	       $port || '',
	       $user, $password, '_admin_internal');
}

package DBD::mysql::db; # ====== DATABASE ======
use strict;

%DBD::mysql::db::db2ANSI = ("INT"   =>  "INTEGER",
			   "CHAR"  =>  "CHAR",
			   "REAL"  =>  "REAL",
			   "IDENT" =>  "DECIMAL"
                          );

### ANSI datatype mapping to mSQL datatypes
%DBD::mysql::db::ANSI2db = ("CHAR"          => "CHAR",
			   "VARCHAR"       => "CHAR",
			   "LONGVARCHAR"   => "CHAR",
			   "NUMERIC"       => "INTEGER",
			   "DECIMAL"       => "INTEGER",
			   "BIT"           => "INTEGER",
			   "TINYINT"       => "INTEGER",
			   "SMALLINT"      => "INTEGER",
			   "INTEGER"       => "INTEGER",
			   "BIGINT"        => "INTEGER",
			   "REAL"          => "REAL",
			   "FLOAT"         => "REAL",
			   "DOUBLE"        => "REAL",
			   "BINARY"        => "CHAR",
			   "VARBINARY"     => "CHAR",
			   "LONGVARBINARY" => "CHAR",
			   "DATE"          => "CHAR",
			   "TIME"          => "CHAR",
			   "TIMESTAMP"     => "CHAR"
			  );

sub prepare {
    my($dbh, $statement, $attribs)= @_;

    # create a 'blank' dbh
    my $sth = DBI::_new_sth($dbh, {'Statement' => $statement});

    # Populate internal handle data.
    if (!DBD::mysql::st::_prepare($sth, $statement, $attribs)) {
	$sth = undef;
    }

    $sth;
}

sub db2ANSI {
    my $self = shift;
    my $type = shift;
    return $DBD::mysql::db::db2ANSI{"$type"};
}

sub ANSI2db {
    my $self = shift;
    my $type = shift;
    return $DBD::mysql::db::ANSI2db{"$type"};
}

sub admin {
    my($dbh) = shift;
    my($command) = shift;
    my($dbname) = ($command eq 'createdb'  ||  $command eq 'dropdb') ?
	shift : '';
    $dbh->{'Driver'}->func($dbh, $command, $dbname, '', '', '',
			   '_admin_internal');
}

sub _SelectDB ($$) {
    die "_SelectDB is removed from this module; use DBI->connect instead.";
}

{
    my $names = ['TABLE_CAT', 'TABLE_SCHEM', 'TABLE_NAME',
		 'TABLE_TYPE', 'REMARKS'];

    sub table_info ($) {
	my $dbh = shift;
	my $sth = $dbh->prepare("SHOW TABLES");
	return undef unless $sth;
	if (!$sth->execute()) {
	  return DBI::set_err($dbh, $sth->err(), $sth->errstr());
        }
	my @tables;
	while (my $ref = $sth->fetchrow_arrayref()) {
	  push(@tables, [ undef, undef, $ref->[0], 'TABLE', undef ]);
        }
	my $dbh2;
	if (!($dbh2 = $dbh->{'~dbd_driver~_sponge_dbh'})) {
	    $dbh2 = $dbh->{'~dbd_driver~_sponge_dbh'} =
		DBI->connect("DBI:Sponge:");
	    if (!$dbh2) {
	        DBI::set_err($dbh, 1, $DBI::errstr);
		return undef;
	    }
	}
	my $sth2 = $dbh2->prepare("SHOW TABLES", { 'rows' => \@tables,
						   'NAME' => $names,
						   'NUM_OF_FIELDS' => 5 });
	if (!$sth2) {
	    DBI::set_err($sth2, $dbh2->err(), $dbh2->errstr());
	}
	$sth2;
    }
}

sub _ListTables {
  my $dbh = shift;
  if (!$DBD::mysql::QUIET) {
    warn "_ListTables is deprecated, use \$dbh->tables()";
  }
  return map { $_ =~ s/.*\.//; $_ } $dbh->tables();
}


sub column_info {
    my ($dbh, $catalog, $schema, $table, $column) = @_;
    return $dbh->set_err(1, "column_info doesn't support table wildcard")
	if $table !~ /^\w+$/;
    return $dbh->set_err(1, "column_info doesn't support column selection")
	if $column ne "%";

    my $table_id = $dbh->quote_identifier($catalog, $schema, $table);

    my @names = qw(
	TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME
	DATA_TYPE TYPE_NAME COLUMN_SIZE BUFFER_LENGTH DECIMAL_DIGITS
	NUM_PREC_RADIX NULLABLE REMARKS COLUMN_DEF
	SQL_DATA_TYPE SQL_DATETIME_SUB CHAR_OCTET_LENGTH
	ORDINAL_POSITION IS_NULLABLE CHAR_SET_CAT
	CHAR_SET_SCHEM CHAR_SET_NAME COLLATION_CAT COLLATION_SCHEM COLLATION_NAME
	UDT_CAT UDT_SCHEM UDT_NAME DOMAIN_CAT DOMAIN_SCHEM DOMAIN_NAME
	SCOPE_CAT SCOPE_SCHEM SCOPE_NAME MAX_CARDINALITY
	DTD_IDENTIFIER IS_SELF_REF
	mysql_is_pri_key mysql_type_name mysql_values
    );
    my %col_info;

    local $dbh->{FetchHashKeyName} = 'NAME_lc';
    my $desc_sth = $dbh->prepare("DESCRIBE $table_id");
    my $desc = $dbh->selectall_arrayref($desc_sth, { Columns=>{} });
    my $ordinal_pos = 0;
    foreach my $row (@$desc) {
	my $type = $row->{type};
	$type =~ m/^(\w+)(?:\((.*?)\))?\s*(.*)/;
	my $basetype = lc($1);

	my $info = $col_info{ $row->{field} } = {
	    TABLE_CAT   => $catalog,
	    TABLE_SCHEM => $schema,
	    TABLE_NAME  => $table,
	    COLUMN_NAME => $row->{field},
	    NULLABLE    => ($row->{null} eq 'YES') ? 1 : 0,
	    IS_NULLABLE => ($row->{null} eq 'YES') ? "YES" : "NO",
	    TYPE_NAME   => uc($basetype),
	    COLUMN_DEF  => $row->{default},
	    ORDINAL_POSITION => ++$ordinal_pos,
	    mysql_is_pri_key => ($row->{key}  eq 'PRI'),
	    mysql_type_name  => $row->{type},
	};
	# This code won't deal with a pathalogical case where a value
	# contains a single quote followed by a comma, and doesn't unescape
	# any escaped values. But who would use those in an enum or set?
	my @type_params = ($2 && index($2,"'")>=0)
			? ("$2," =~ /'(.*?)',/g)  # assume all are quoted
			: split /,/, $2||'';      # no quotes, plain list
	s/''/'/g for @type_params;                # undo doubling of quotes
	my @type_attr = split / /, $3||'';
	#warn "$type: $basetype [@type_params] [@type_attr]\n";

	$info->{DATA_TYPE} = SQL_VARCHAR();
	if ($basetype =~ /char|text|blob/) {
	    $info->{DATA_TYPE} = SQL_CHAR() if $basetype eq 'char';
	    if ($type_params[0]) {
		$info->{COLUMN_SIZE} = $type_params[0];
	    }
	    else {
		$info->{COLUMN_SIZE} = 65535;
		$info->{COLUMN_SIZE} = 255        if $basetype =~ /^tiny/;
		$info->{COLUMN_SIZE} = 16777215   if $basetype =~ /^medium/;
		$info->{COLUMN_SIZE} = 4294967295 if $basetype =~ /^long/;
	    }
	}
	elsif ($basetype =~ /enum|set/) {
	    if ($basetype eq 'set') {
		$info->{COLUMN_SIZE} = length(join ",", @type_params);
	    }
	    else {
		my $max_len = 0;
		length($_) > $max_len and $max_len = length($_) for @type_params;
		$info->{COLUMN_SIZE} = $max_len;
	    }
	    $info->{"mysql_values"} = \@type_params;
	}
	elsif ($basetype =~ /int/) {
	    $info->{DATA_TYPE} = SQL_INTEGER();
	    $info->{NUM_PREC_RADIX} = 10;
	    $info->{COLUMN_SIZE} = $type_params[0];
	}
	elsif ($basetype =~ /decimal/) {
	    $info->{DATA_TYPE} = SQL_DECIMAL();
	    $info->{NUM_PREC_RADIX} = 10;
	    $info->{COLUMN_SIZE}    = $type_params[0];
	    $info->{DECIMAL_DIGITS} = $type_params[1];
	}
	elsif ($basetype =~ /float|double/) {
	    $info->{DATA_TYPE} = ($basetype eq 'float') ? SQL_FLOAT() : SQL_DOUBLE();
	    $info->{NUM_PREC_RADIX} = 2;
	    $info->{COLUMN_SIZE} = ($basetype eq 'float') ? 32 : 64;
	}
	elsif ($basetype =~ /date|time/) { # date/datetime/time/timestamp
	    if ($basetype eq 'time' or $basetype eq 'date') {
		$info->{DATA_TYPE}   = ($basetype eq 'time') ? SQL_TYPE_TIME() : SQL_TYPE_DATE();
		$info->{COLUMN_SIZE} = ($basetype eq 'time') ? 8 : 10;
	    }
	    else { # datetime/timestamp
		$info->{DATA_TYPE}     = SQL_TYPE_TIMESTAMP();
		$info->{SQL_DATA_TYPE} = SQL_DATETIME();
	        $info->{SQL_DATETIME_SUB} = $info->{DATA_TYPE} - ($info->{SQL_DATA_TYPE} * 10);
		$info->{COLUMN_SIZE}   = ($basetype eq 'datetime') ? 19 : $type_params[0] || 14;
	    }
	    $info->{DECIMAL_DIGITS} = 0; # no fractional seconds
	}
	else {
	    warn "unsupported column '$row->{field}' type '$basetype' treated as varchar";
	}
	$info->{SQL_DATA_TYPE} ||= $info->{DATA_TYPE};
	#warn Dumper($info);
    }

    my $sponge = DBI->connect("DBI:Sponge:", '','')
	or return $dbh->DBI::set_err($DBI::err, "DBI::Sponge: $DBI::errstr");
    my $sth = $sponge->prepare("column_info $table", {
	rows => [ map { [ @{$_}{@names} ] } values %col_info ],
	NUM_OF_FIELDS => scalar @names,
	NAME => \@names,
    }) or return $dbh->DBI::set_err($sponge->err(), $sponge->errstr());

    return $sth;
}



####################
# get_info()
# Generated by DBI::DBD::Metadata

sub get_info {
    my($dbh, $info_type) = @_;
    require DBD::mysql::GetInfo;
    my $v = $DBD::mysql::GetInfo::info{int($info_type)};
    $v = $v->($dbh) if ref $v eq 'CODE';
    return $v;
}



package DBD::mysql::st; # ====== STATEMENT ======
use strict;

1;
