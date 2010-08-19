package org.scale7.demo;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.scale7.cassandra.pelops.*;
import org.scale7.cassyndex.Cassyndex;
import org.scale7.cassyndex.CisKeyOnlyIndex;
import org.scale7.cassyndex.FullTextIndex;
import org.scale7.cassyndex.FullTextIndex.TextTransform;
import org.scale7.cassyndex.IKeyIterator;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;


public class App
{
	protected static boolean isAlreadyExistsException(Exception ex) {
		if (!(ex instanceof InvalidRequestException))
			return false;
		InvalidRequestException ire = (InvalidRequestException)ex;
		if (ire.why.toLowerCase().contains("already exists"))
			return true;
		return false;
	}

	protected static boolean isAlreadyDefinedException(Exception ex) {
		if (!(ex instanceof InvalidRequestException))
			return false;
		InvalidRequestException ire = (InvalidRequestException)ex;
		if (ire.why.toLowerCase().contains("already defined in that keyspace"))
			return true;
		return false;
	}

    public static void main( String[] args ) throws Exception
    {
    	final Logger logger = SystemProxy.getLoggerFromFactory(App.class);
    	try {
    		// Describe our cluster
    		Cluster cluster = new Cluster(new String[] { "127.0.0.1" }, 9160);
    		cluster.setFramedTransportRequired(false);

    		// Get some info about the cluster
    		ClusterManager clusterManager = Pelops.createClusterManager(cluster);
    		logger.info("Connected to {}", clusterManager.getClusterName());
    		logger.info("Running Cassandra version {}", clusterManager.getCassandraVersion());

    		// Create a new keyspace on the cluster
    		try {
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
    		} catch (Exception ex) {
    			if (!isAlreadyExistsException(ex))
    				throw ex;
    		}

    		// Add an additional column family (this one already added in previous statements!)
    		try {
		    	CfDef columnFamilyDefinition = new CfDef("Cassyndex", "CisNameIndex");
		    	columnFamilyDefinition.column_type = ColumnFamilyManager.CFDEF_TYPE_STANDARD;
		    	columnFamilyDefinition.comparator_type = ColumnFamilyManager.CFDEF_COMPARATOR_UTF8;
		    	ColumnFamilyManager columnFamilyManager = Pelops.createColumnFamilyManager(cluster, "Cassyndex");
		    	columnFamilyManager.addColumnFamily(columnFamilyDefinition);
    		} catch (Exception ex) {
    			if (!isAlreadyDefinedException(ex))
    				throw ex;
    		}

    		// Add an additional column family
    		try {
		    	CfDef columnFamilyDefinition = new CfDef("Cassyndex", "FullTextAddressIndex");
		    	columnFamilyDefinition.column_type = ColumnFamilyManager.CFDEF_TYPE_STANDARD;
		    	columnFamilyDefinition.comparator_type = ColumnFamilyManager.CFDEF_COMPARATOR_UTF8;
		    	ColumnFamilyManager columnFamilyManager = Pelops.createColumnFamilyManager(cluster, "Cassyndex");
		    	columnFamilyManager.addColumnFamily(columnFamilyDefinition);
    		} catch (Exception ex) {
    			if (!isAlreadyDefinedException(ex))
    				throw ex;
    		}

    		// Create pool for operations
    		CachePerNodePool.Policy policy = new CachePerNodePool.Policy();
    		policy.setDynamicNodeDiscovery(true);
    		logger.info("Starting connection pool...");
	    	Pelops.addPool("main", cluster, "Cassyndex", new OperandPolicy(), policy);

    		// Write to our key only index
	    	CisKeyOnlyIndex index = Cassyndex.createCisKeyOnlyIndex("main", new CisKeyOnlyIndex.Config("CisNameIndex"));
	    	logger.info("Writing some entries to key only index...");
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

	    	// Check if a key exists
	    	boolean exists = index.keyExists("dominic williams", ConsistencyLevel.QUORUM);
	    	logger.info("Expected key found: {}", exists);

	    	// Iterate over a section of the key index
	    	String searchPrefix = "Matt Grogan";
	    	logger.info("Searching index from: {}", searchPrefix);
	    	IKeyIterator iterator = index.getIterator(searchPrefix, true, 3, ConsistencyLevel.QUORUM);
	    	while (iterator.hasNext()) {
	    		String[] keys = iterator.next();
	    		for (String key : keys)
	    			logger.info("Found name: {}", key);
	    	}

	    	// Write some entries to our full text index
	    	String[] blockWords = new String[] {"the", "school", "college", "road", "drive", "street"};
	    	FullTextIndex.Config config = new FullTextIndex.Config("FullTextAddressIndex");
	    	config.setBlockWords(blockWords);
	    	FullTextIndex fullTextIndex = Cassyndex.createFullTextIndex("main", config);
	    	ConsistencyLevel cLevel = ConsistencyLevel.QUORUM;
	    	fullTextIndex.addItem("school1", "Fairwater Primary School, Wellwright Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school2", "Gabalfa Primary School, Colwill Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school3", "Kitchener Primary School, Kitchener Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school4", "Lansdowne Primary School, Norfolk Street, Cardiff", cLevel);
	    	fullTextIndex.addItem("school5", "Marlborough Junior School, Blenheim Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school6", "Marlborough  Infant School,Marlborough Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school7", "Moorland Primary School, Singleton Road,   Cardiff", cLevel);
	    	fullTextIndex.addItem("school8", "Radnor Primary School, Radnor Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school9", "Rhydypenau Primary School, Fidlas Avenue, Cardiff", cLevel);
	    	fullTextIndex.addItem("school10", "Roath Park Primary School, Penywain Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school11", "Greenway Primary School, Llanstephan Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school12", "Stacey Primary School, Stacey Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school13", "Ton-Yr-Ywen Primary School, Maes-Y-Coed Road", cLevel);
	    	fullTextIndex.addItem("school14", "Caerau Infant School, Caerau Lane, Cardiff", cLevel);
	    	fullTextIndex.addItem("school15", "Peter Lea Primary School, Carter Place, Cardiff", cLevel);
	    	fullTextIndex.addItem("school16", "Cefn Onn Primary School, Llangranog Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school17", "Bryn Hafod Primary School, Blagdon Close, Cardiff", cLevel);
	    	fullTextIndex.addItem("school18", "Cwrt-Yr-Ala Junior School, Cyntwell Avenue, Cardiff", cLevel);
	    	fullTextIndex.addItem("school19", "Pen-Y-Bryn Primary School, Dunster Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school20", "Coed Glas Primary School,Ty Glas Avenue, Cardiff", cLevel);
	    	fullTextIndex.addItem("school21", "Lakeside Primary School, Ontario Way, Cardiff", cLevel);
	    	fullTextIndex.addItem("school22", "Pentrebane Primary School,Beechley Drive,Fairwater, Cardiff", cLevel);
	    	fullTextIndex.addItem("school23", "Trowbridge Infant School, Glan-Y-Mor Close, Rumney, Cardiff", cLevel);
	    	fullTextIndex.addItem("school24", "Mount Stuart Primary, Adelaide Street, Cardiff", cLevel);
	    	fullTextIndex.addItem("school25", "Eglwys Newydd Primary School, Glan-Y-Nant Road, Cardiff", cLevel);
	    	fullTextIndex.addItem("school26", "Lakeside High, Hampstead Avenue, London", cLevel);
	    	fullTextIndex.addItem("school27", "Lakeside Junior, Hampstead Avenue, London", cLevel);
	    	fullTextIndex.addItem("school28", "Moorcroft Upper School,\nEton Avenue,\r\nLondon", cLevel);
	    	fullTextIndex.addItem("school29", "Moorhouse Upper School, Park Street, Birmingham", cLevel);
	    	fullTextIndex.addItem("school30", "Moor Lantern Upper School, New Street, Newcastle", cLevel);

	    	// Perform a search on the full text index
	    	String[] itemIds = fullTextIndex.findItems("Moor Upper", 20, cLevel);
	    	// Transform the item ids into item text for display
	    	TextTransform transform = new TextTransform();
	    	transform.setReplaceLineBreaks(true); // so can display output on a single line e.g. in combo box
	    	String[] results = fullTextIndex.itemIdsToText(itemIds, 10, transform, cLevel);
	    	// Iterate and display results
	    	for (String result : results) {
	    		logger.info("Found school: " + result);
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
