[![.github/workflows/ci.yml](https://github.com/perl5-dbi/DBD-mysql/actions/workflows/ci.yml/badge.svg)](https://github.com/perl5-dbi/DBD-mysql/actions/workflows/ci.yml)

# DBD::mysql - database driver for Perl

This is the Perl [DBI](https://metacpan.org/pod/DBI) driver for access to MySQL and MySQL Compatible databases.

## Usage

Usage is described in [DBD::mysql](https://metacpan.org/pod/DBD::mysql).

## Building and Testing

For building DBD::mysql you need the MySQL 8.x client library.

```
perl Makefile.PL
make
make test
```

See the output of `perl Makefile.PL` for how to set database credentials.

Testing is also done via GitHub action.

## Installation

Installation is described in [DBD::mysql::INSTALL](https://metacpan.org/pod/DBD::mysql::INSTALL).

## Support

This module is maintained and supported on a mailing list, dbi-users.
To subscribe to this list, send an email to

    dbi-users-subscribe@perl.org

Mailing list archives are at

[http://groups.google.com/group/perl.dbi.users](http://groups.google.com/group/perl.dbi.users)
