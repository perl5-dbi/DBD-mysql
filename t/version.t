use strict;
use warnings;

use DBD::mysql;
use Test::More;

like($DBD::mysql::VERSION, qr/^\d\.\d{2,3}(|_\d\d)$/, 'version format');
like($DBD::mysql::VERSION, qr/^5\./, 'version starts with "5." (update for 6.x)');

diag("mysql_get_client_version: ", DBD::mysql->client_version);
cmp_ok(DBD::mysql->client_version, ">", 0, "mysql_get_client_version is available as a standalone function");

done_testing;
