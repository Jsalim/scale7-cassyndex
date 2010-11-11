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

public class CaseSenKeyIndex extends KeyIndexBase implements IKeyIndex {

	protected CaseSenKeyIndex(String pelopsPool, Config config) {
		super(pelopsPool, config);
	}

	@Override
	public boolean keyExists(String key, ConsistencyLevel cLevel) throws Exception {
		VALIDATE(key);
		return selector.getColumnCount(config.idxColumnFamily, getBucketRowKey(key, config.bucketKeyPrefixLen, 0), Selector.newColumnsPredicate(key, key, false, 100), cLevel) == 1;
	}

	@Override
	public void writeKey(String key, ConsistencyLevel cLevel) throws Exception {
		VALIDATE(key);
		Mutator mutator = Pelops.createMutator(pelopsPool);
		mutator.writeColumn(config.idxColumnFamily, getBucketRowKey(key, config.bucketKeyPrefixLen, 0), mutator.newColumn(key, Bytes.EMPTY));
		mutator.execute(cLevel);
	}

	@Override
	public void deleteKey(String key, ConsistencyLevel cLevel) throws Exception {
		VALIDATE(key);
		Mutator mutator = Pelops.createMutator(pelopsPool);
		mutator.deleteColumn(config.idxColumnFamily, getBucketRowKey(key, config.bucketKeyPrefixLen, 0), key);
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

	private class Iterator implements IKeyIterator {

		private String bucketRowKey;
		private Bytes startColName;
		private Bytes stopColName;
		private boolean reversed;
		private int maxPageSize;
		private ConsistencyLevel cLevel;
		private String[] firstPage;
		private String[] currentPage;

		/*
		protected Iterator(String startKey, String stopKey, int maxPageSize, ConsistencyLevel cLevel) {
			this.startColName = Bytes.fromUTF8(startKey);
			reversed = startKey.compareTo(stopKey) != -1;
			if (!reversed) {
				stopColName = Bytes.fromUTF8(stopKey + Character.MAX_VALUE);
			} else {
				stopColName = Selector.bumpUpColumnName(stopKey, OrderType.UTF8Type);
			}
			bucketRowKey = getBucketRowKey(startKey, config.bucketKeyPrefixLen, 0);
			this.maxPageSize = maxPageSize;
			this.cLevel = cLevel;

		}*/

		protected Iterator(String requiredPrefix, boolean reversed, int maxPageSize, ConsistencyLevel cLevel) {
			if (!reversed) {
				this.startColName = Bytes.fromUTF8(requiredPrefix);
				stopColName = Bytes.fromUTF8(requiredPrefix + Character.MAX_VALUE);
			} else {
				this.startColName = Bytes.fromUTF8(requiredPrefix + Character.MAX_VALUE);
				stopColName = Bytes.fromUTF8(requiredPrefix);
			}
			bucketRowKey = getBucketRowKey(requiredPrefix, config.bucketKeyPrefixLen, 0);
			this.reversed = reversed;
			this.maxPageSize = maxPageSize;
			this.cLevel = cLevel;

		}

		@Override
		public boolean hasNext() throws Exception {
			if (currentPage == null) {
				firstPage = getPageOfColNamesAsKeys(startColName, stopColName);
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
			String lastEntry = currentPage[maxPageSize-1];

			List<Column> columns = selector.getPageOfColumnsFromRow(config.idxColumnFamily, bucketRowKey, lastEntry, reversed, maxPageSize, cLevel);

			return convertColNamesToKeys(columns);
		}

		private String[] getPageOfColNamesAsKeys(Bytes startColName, Bytes stopColName) throws Exception {
			SlicePredicate predicate = Selector.newColumnsPredicate(startColName, stopColName, reversed, maxPageSize);
			List<Column> columns = selector.getColumnsFromRow(config.idxColumnFamily, bucketRowKey, predicate, cLevel);
			return convertColNamesToKeys(columns);
		}

		private String[] convertColNamesToKeys(List<Column> columns) {
			List<String> result = new ArrayList<String>(columns.size());
			for (Column column : columns)
				result.add(Bytes.fromBytes(column.getName()).toUTF8());
			return result.toArray(new String[]{});
		}
	};
}
