package org.scale7.cassyndex;

import org.apache.cassandra.thrift.ConsistencyLevel;

public interface IKeyOnlyIndex {
	boolean keyExists(String key, ConsistencyLevel cLevel) throws Exception;
	
	void writeKey(String key, ConsistencyLevel cLevel) throws Exception;

	void deleteKey(String key, ConsistencyLevel cLevel) throws Exception;
	
	IKeyIterator getIterator(String requiredPrefix, boolean reversed, int maxPageSize, ConsistencyLevel cLevel) throws Exception;

	String[] getPage(String requiredPrefix, String startKey,
			boolean includeStartKey, boolean reversed, int maxPageSize,
			ConsistencyLevel cLevel) throws Exception;
}
