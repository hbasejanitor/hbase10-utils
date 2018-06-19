## Hbase proxy for flume

This is a flume source that receives hbase mutation events and forwards 
them to the flume channel of your choice.

The source creates a replication peer with your hbase cluster and 
receives the mutation events as they are sent out by the region servers. The
work will be spread out among all the flume agents, so the more agents you 
run, the better.

### How to build
the standard "mvn clean install" will build a tarball, If you download the maven
parcel plugin: https://github.com/hbasejanitor/maven-parcel-plugin you can
type "mvn clean parcel:makeparcel" a parcel will be built for easy deploloyment
to a cloudera managed cluster.

###How to configure

The source follows the mormal flume connventions for configuration.

rules come in this form:


<agent id\>.rule.\<rule id\>.keep=true/false

\<agent id\>.rule.\<rule id\>.rules.columnfamily=column family

\<agent id\>.rule.\<rule id\>.rules.qualifier=qualifier

\<agent id\>.rule.\<rule id\>.rules.table=table


 - keep true/false whether anything that matches this rule should be dropped or kept
 - columnfamily match on column family
 - qualifier match on qualifier
 - table match on table
 
 Note: keep=false rules are always evaluated first.
 
 Example:
 
tier1.sources  = source1
tier1.channels = channel1
tier1.sinks    = sink1
tier1.sources.source1.rule.1.keep=true
tier1.sources.source1.rule.1.rules.table=default:MikeFoo
 
The above snippet configures the agent to send all mutations from table MikeFoo to the flume channel.
 
tier1.sources  = source1
tier1.channels = channel1
tier1.sinks    = sink1
tier1.sources.source1.rule.1.keep=true
tier1.sources.source1.rule.1.rules.table=default:MikeFoo
tier1.sources.source1.rule.2.keep=false
tier1.sources.source1.rule.2.rules.table=default:MikeFoo
tier1.sources.source1.rule.2.rules.qualifier=donotforward

The above snippet configures the agent to send all mutations from table MikeFoo except the qualifier donotforward to the flume channel.


###Additional Configuration

tier1.sources.source1.hbase.peer=flumehbase

Use the peer named flumehbase.  DO NOT USE A PEER THAT IS FORWARDING EVENTS TO ANOTHER HBASE CLUSER.

tier1.sources.source1.hbase.enablepeer=true

Enable the peer automatically at startup.

This hasn't been tested on secure clusters yet, feedback is welcome =-)
 
 hbasejanitor@gmail.com
 