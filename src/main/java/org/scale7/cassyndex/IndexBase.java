package org.scale7.cassyndex;

import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

public abstract class IndexBase {

	public static class Config {
		protected int bucketKeyPrefixLen;
		protected String idxColumnFamily;

		public Config(String idxColumnFamily) {
			this(idxColumnFamily, 1);
		}

		public Config(String idxColumnFamily, int bucketKeyPrefixLen) {
			this.idxColumnFamily = idxColumnFamily;
			this.bucketKeyPrefixLen = bucketKeyPrefixLen;
		}
	}

	protected String pelopsPool;
	protected Config config;
	protected Selector selector;

	protected IndexBase(String pelopsPool, Config config) {
		this.pelopsPool = pelopsPool;
		this.config = config;
		selector = Pelops.createSelector(pelopsPool);
	}

	protected String getBucketRowKey(String keyPrefix, int prefixLength, int bucketIdx) {
		String bucketKey = keyPrefix.substring(0, keyPrefix.length() < prefixLength ? keyPrefix.length() : prefixLength);
		while (bucketKey.length() < prefixLength)
			bucketKey = bucketKey + Character.MIN_VALUE;
		bucketKey = bucketKey + bucketIdx;
		return bucketKey;
	}

	protected void VALIDATE(String keyPrefix) throws Exception {
		if (keyPrefix.length() < config.bucketKeyPrefixLen)
			throw new Exception("This index only supports searching for keys with prefixes equal to or larger than: " + config.bucketKeyPrefixLen);
	}
}
