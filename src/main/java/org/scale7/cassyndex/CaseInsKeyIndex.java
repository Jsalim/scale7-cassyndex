package org.scale7.cassyndex;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.Selector.OrderType;

import static org.scale7.cassandra.pelops.StringHelper.*;

public class CaseInsKeyIndex extends KeyIndexBase implements IKeyIndex {

	public static class Config extends KeyIndexBase.Config {

		protected boolean fullCaseKeys = true; // normally search is case insensitive, but keys are returned with original case

		public Config(String idxColumnFamily) {
			this(idxColumnFamily, 2);
		}

		public Config(String idxColumnFamily, int bucketKeyPrefixLen) {
			super(idxColumnFamily, bucketKeyPrefixLen);
		}
		
		public void setFullCaseKeys(boolean fullCaseKeys) {
			this.fullCaseKeys = fullCaseKeys;
		}
	};
	
	protected CaseInsKeyIndex(String pelopsPool, Config config) {
		super(pelopsPool, config);
	}

	@Override
	public boolean keyExists(String key, ConsistencyLevel cLevel) throws Exception {
		VALIDATE(key);
		String lcKey= key.toLowerCase();
		return selector.getColumnCount(config.idxColumnFamily, getBucketRowKey(lcKey, config.bucketKeyPrefixLen, 0), Selector.newColumnsPredicate(lcKey, lcKey, false, 100), cLevel) == 1;
	}

	@Override
	public void writeKey(String key, ConsistencyLevel cLevel) throws Exception {
		VALIDATE(key);
		String lcKey= key.toLowerCase();
		Mutator mutator = Pelops.createMutator(pelopsPool);
		if (((CaseInsKeyIndex.Config)config).fullCaseKeys)
			mutator.writeColumn(config.idxColumnFamily, getBucketRowKey(lcKey, config.bucketKeyPrefixLen, 0), mutator.newColumn(lcKey, key));
		else
			mutator.writeColumn(config.idxColumnFamily, getBucketRowKey(lcKey, config.bucketKeyPrefixLen, 0), mutator.newColumn(lcKey, ""));
		mutator.execute(cLevel);
	}

	@Override
	public void deleteKey(String key, ConsistencyLevel cLevel) throws Exception {
		VALIDATE(key);
		String lcKey= key.toLowerCase();
		Mutator mutator = Pelops.createMutator(pelopsPool);
		mutator.deleteColumn(config.idxColumnFamily, getBucketRowKey(lcKey, config.bucketKeyPrefixLen, 0), lcKey);
		mutator.execute(cLevel);
	}

	@Override
	public Iterator getIterator(String requiredPrefix, boolean reversed, int maxPageSize, ConsistencyLevel cLevel) throws Exception {
		VALIDATE(requiredPrefix);
		return new Iterator(requiredPrefix, reversed, maxPageSize, cLevel);
	}

	@Override
	public String[] getPage(String requiredPrefix, String startKey, boolean includeStartKey, boolean reversed, int maxPageSize, ConsistencyLevel cLevel) throws Exception {
		VALIDATE(requiredPrefix);
		return null;
	}

	/*
	public String[] getPageFrom(String searchPrefix, boolean inclusive, boolean reversed, int maxPageSize, ConsistencyLevel cLevel) {
		String bucketRowKey;
		Bytes startColName;
		Bytes stopColName;
		String lcSearchPrefix = searchPrefix.toLowerCase();
		if (!reversed) {
			startColName = Bytes.fromUTF8(lcSearchPrefix);
			stopColName = Bytes.fromUTF8(lcSearchPrefix + Character.MAX_VALUE);
		} else {
			startColName = Bytes.fromUTF8(lcSearchPrefix + Character.MAX_VALUE);
			stopColName = Bytes.fromUTF8(lcSearchPrefix.length() > 1 ? lcSearchPrefix.substring(0, lcSearchPrefix.length()-1) : "");
		}
		return getPageOfColNamesAsKeys(startColName, stopColName);
	}
	*/
	private class Iterator implements IKeyIterator {

		private String bucketRowKey;
		private Bytes startColName;
		private Bytes stopColName;
		private boolean reversed;
		private int maxPageSize;
		private ConsistencyLevel cLevel;
		private String[] firstPage;
		private String[] currentPage;

		protected Iterator(String requiredPrefix, boolean reversed, int maxPageSize, ConsistencyLevel cLevel) {
			String lcRequiredPrefix = requiredPrefix.toLowerCase();
			if (!reversed) {
				this.startColName = Bytes.fromUTF8(lcRequiredPrefix);
				stopColName = Bytes.fromUTF8(lcRequiredPrefix + Character.MAX_VALUE);
			} else {
				this.startColName = Bytes.fromUTF8(lcRequiredPrefix + Character.MAX_VALUE);
				stopColName = Bytes.fromUTF8(lcRequiredPrefix);
			}
			bucketRowKey = getBucketRowKey(lcRequiredPrefix, config.bucketKeyPrefixLen, 0);
			this.reversed = reversed;
			this.maxPageSize = maxPageSize;
			this.cLevel = cLevel;

		}

		@Override
		public boolean hasNext() throws Exception {
			if (currentPage == null) {
				firstPage = getPageOfKeys(startColName, stopColName);
				return true;
			}

			if (currentPage.length >= maxPageSize)
				return true;

			return false;
		}

		@Override
		public String[] next() throws Exception {
			if (firstPage != null) {
				currentPage = firstPage;
				firstPage = null;
				return currentPage;
			}
			String lcLastEntry = currentPage[maxPageSize-1].toLowerCase();
			Bytes continueFrom = reversed ? Selector.bumpDownColumnName(lcLastEntry, OrderType.UTF8Type)
					: Selector.bumpUpColumnName(lcLastEntry, OrderType.UTF8Type);
			
			currentPage = getPageOfKeys(continueFrom, stopColName);

			return currentPage;
		}
		
		private String[] getPageOfKeys(Bytes startColName, Bytes stopColName) throws Exception {
			if (((CaseInsKeyIndex.Config)config).fullCaseKeys)
				return getPageOfColValuesAsKeys(startColName, stopColName);
			
			return getPageOfColNamesAsKeys(startColName, stopColName);
		}

		private String[] getPageOfColNamesAsKeys(Bytes startColName, Bytes stopColName) throws Exception {
			List<Column> columns = getPageOfColumns(startColName, stopColName);
			List<String> result = new ArrayList<String>(columns.size());
			for (Column column : columns)
				result.add(toUTF8(column.name));
			return result.toArray(new String[]{});
		}

		private String[] getPageOfColValuesAsKeys(Bytes startColName, Bytes stopColName) throws Exception {
			return convertColValuesToKeys(getPageOfColumns(startColName, stopColName));
		}

		private List<Column> getPageOfColumns(Bytes startColName, Bytes stopColName) throws Exception {
			SlicePredicate predicate = Selector.newColumnsPredicate(startColName, stopColName, reversed, maxPageSize);
			List<Column> columns = selector.getColumnsFromRow(config.idxColumnFamily, bucketRowKey, predicate, cLevel);
			return columns;
		}

		private String[] convertColValuesToKeys(List<Column> columns) {
			List<String> result = new ArrayList<String>(columns.size());
			for (Column column : columns)
				result.add(toUTF8(column.value));
			return result.toArray(new String[]{});
		}
	};
}
