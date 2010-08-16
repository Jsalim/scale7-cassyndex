package org.scale7.demo;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.scale7.cassandra.pelops.*;
import org.scale7.cassyndex.Cassyndex;
import org.scale7.cassyndex.CisKeyOnlyIndex;
import org.scale7.cassyndex.CsKeyOnlyIndex;
import org.scale7.cassyndex.IKeyIterator;
import org.scale7.cassyndex.IndexBase;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;


public class App
{
    public static void main( String[] args ) throws Exception
    {
    	final Logger logger = SystemProxy.getLoggerFromFactory(App.class);
    	try {
    		boolean createDatabase = false;
    		boolean createExtraColumnFamily = false;

    		// Describe our cluster
    		Cluster cluster = new Cluster(new String[] { "127.0.0.1" }, 9160);
    		cluster.setFramedTransportRequired(false);

    		// Get some info about the cluster
    		ClusterManager clusterManager = Pelops.createClusterManager(cluster);
    		logger.info("Connected to {}", clusterManager.getClusterName());
    		logger.info("Running Cassandra version {}", clusterManager.getCassandraVersion());

    		// Create a new keyspace on the cluster
    		if (createDatabase) {
    			logger.info("Creating keyspace...");
    			// build schema
	    		List<CfDef> columnFamilyDefinitions = new ArrayList<CfDef>(100);
		    	CfDef columnFamilyDefinition = new CfDef("Cassyndex", "NameIndex");
		    	columnFamilyDefinition.column_type = ColumnFamilyManager.CFDEF_TYPE_STANDARD;
		    	columnFamilyDefinition.comparator_type = ColumnFamilyManager.CFDEF_COMPARATOR_UTF8;
	    		columnFamilyDefinitions.add(columnFamilyDefinition);
	    		KsDef keyspaceDefinition = new KsDef("Cassyndex", KeyspaceManager.KSDEF_STRATEGY_RACK_UNAWARE, 1, columnFamilyDefinitions);
	    		// create keyspace
	    		KeyspaceManager keyspaceManager = Pelops.createKeyspaceManager(cluster);
	    		keyspaceManager.addKeyspace(keyspaceDefinition);
    		}

    		if (createExtraColumnFamily) {
		    	CfDef columnFamilyDefinition = new CfDef("Cassyndex", "CisNameIndex");
		    	columnFamilyDefinition.column_type = ColumnFamilyManager.CFDEF_TYPE_STANDARD;
		    	columnFamilyDefinition.comparator_type = ColumnFamilyManager.CFDEF_COMPARATOR_UTF8;
		    	ColumnFamilyManager columnFamilyManager = Pelops.createColumnFamilyManager(cluster, "Cassyndex");
		    	columnFamilyManager.addColumnFamily(columnFamilyDefinition);
    		}

    		// Create pool for operations
    		logger.info("Starting connection pool...");
    		CachePerNodePool.Policy policy = new CachePerNodePool.Policy();
    		policy.setDynamicNodeDiscovery(true);
	    	Pelops.addPool("main", cluster, "Cassyndex", new OperandPolicy(), policy);

    		// Create key only index
	    	logger.info("Writing some entries to key only index...");
	    	CisKeyOnlyIndex index = Cassyndex.createCisKeyOnlyIndex("main", new IndexBase.Config("CisNameIndex"));
	    	index.writeKey("Dominic Williams", ConsistencyLevel.QUORUM);
	    	index.writeKey("Matt Grogan", ConsistencyLevel.QUORUM);
	    	index.writeKey("Mark Twain", ConsistencyLevel.QUORUM);
	    	index.writeKey("michael mates", ConsistencyLevel.QUORUM);
	    	index.writeKey("Michael Jackson", ConsistencyLevel.QUORUM);
	    	index.writeKey("James Hibberd", ConsistencyLevel.QUORUM);
	    	index.writeKey("Alexis de Werra", ConsistencyLevel.QUORUM);
	    	index.writeKey("mavis bullock", ConsistencyLevel.QUORUM);
	    	index.writeKey("Shaheman Farid", ConsistencyLevel.QUORUM);
	    	index.writeKey("Melissa Williams", ConsistencyLevel.QUORUM);
	    	index.writeKey("Jon Ball", ConsistencyLevel.QUORUM);
	    	index.writeKey("Dylan Williams", ConsistencyLevel.QUORUM);
	    	index.writeKey("Charlie Williams", ConsistencyLevel.QUORUM);
	    	index.writeKey("Martin Lloyd-Elliot", ConsistencyLevel.QUORUM);
	    	index.writeKey("Sacha Williams", ConsistencyLevel.QUORUM);
	    	index.writeKey("Theo Williams", ConsistencyLevel.QUORUM);
	    	index.writeKey("Mark Antony", ConsistencyLevel.QUORUM);
	    	index.writeKey("Matthew Fisher", ConsistencyLevel.QUORUM);

	    	// See if something exists
	    	boolean exists = index.keyExists("Dominic Williams", ConsistencyLevel.QUORUM);
	    	logger.info("Expected key found: {}", exists);

	    	// Grab a section of index
	    	String searchPrefix = "mat";
	    	logger.info("Searching index from: {}", searchPrefix);
	    	IKeyIterator iterator = index.getIterator(searchPrefix, true, 3, ConsistencyLevel.QUORUM);
	    	while (iterator.hasNext()) {
	    		String[] keys = iterator.next();
	    		for (String key : keys)
	    			logger.info("Found key: {}", key);
	    	}

	    	// Wait for console input
	    	//logger.info("Hit <RETURN> to close...");
	    	//(new Scanner(System.in)).nextLine();

		} catch (Exception unknownEx) {
			StringWriter sw = new StringWriter();
		    PrintWriter pw = new PrintWriter(sw);
		    unknownEx.printStackTrace(pw);
			logger.error("Encountered exception: {}", unknownEx);
		}
		// Bye bye
		Pelops.shutdown();
    }
}
