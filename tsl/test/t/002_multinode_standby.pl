# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# test a multi node cluster with queries from access node standby
use strict;
use warnings;
use AccessNode;
use DataNode;
use TestLib;
use Test::More tests => 8;

#Initialize all the multi-node instances

my $an = AccessNode->get_new_node('an');
$an->init(
	allows_streaming => 1,
	auth_extra       => [ '--create-role', 'repl_role' ]);
$an->start;
$an->safe_psql('postgres', 'CREATE EXTENSION timescaledb');

my $backup_name = 'my_backup';

# Take backup
$an->backup($backup_name);

# Create streaming standby linking to master
# Note that we disable 2PC on the standby to allow READ queries from it
my $an_standby = AccessNode->get_new_node('an_standby_1');
$an_standby->init_from_backup($an, $backup_name, has_streaming => 1);
$an_standby->append_conf(
	'postgresql.conf', qq(
    timescaledb.enable_2pc = false
));
$an_standby->start;

#Initialize and set up data nodes now
my $dn1 = DataNode->create('dn1');
my $dn2 = DataNode->create('dn2');

$an->add_data_node($dn1);
$an->add_data_node($dn2);

#Create a distributed hypertable and insert a few rows
$an->safe_psql(
	'postgres',
	qq[
    CREATE TABLE test(time timestamp NOT NULL, device int, temp float);
    SELECT create_distributed_hypertable('test', 'time', 'device', 3);
    INSERT INTO test SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;
    ]);

#Allow standby to catch up with the primary
$an->wait_for_catchup($an_standby, 'replay');

#Check that chunks are shown appropriately from the AN standby node
my $query = q[SELECT * from show_chunks('test');];

#Query Access Standby node
$an_standby->psql_is(
	'postgres', $query, q[_timescaledb_internal._dist_hyper_1_1_chunk
_timescaledb_internal._dist_hyper_1_2_chunk
_timescaledb_internal._dist_hyper_1_3_chunk
_timescaledb_internal._dist_hyper_1_4_chunk],
	'AN Standby shows correct set of chunks');

#Check that SELECT queries work ok from the AN standby node
my $result = $an_standby->safe_psql('postgres', "SELECT count(*) FROM test");
is($result, qq(145), 'streamed content on AN standby');

# Check that only READ-only queries can run on AN standby node
my ($ret, $stdout, $stderr) =
  $an_standby->psql('postgres', 'INSERT INTO test(time) VALUES (now())');
is($ret, qq(3), "failed as expected");
like(
	$stderr,
	qr/cannot execute INSERT in a read-only transaction/,
	"read only message as expected");

# Check that queries which connect to datanodes also remain read only
($ret, $stdout, $stderr) =
  $an_standby->psql('postgres',
	'CALL distributed_exec($$ CREATE USER testrole WITH LOGIN $$)');
is($ret, qq(3), "failed as expected");
like(
	$stderr,
	qr/cannot execute CREATE ROLE in a read-only transaction/,
	"read only message for DNs as expected");

done_testing();

1;
