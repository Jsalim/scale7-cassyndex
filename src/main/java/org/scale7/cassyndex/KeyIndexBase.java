package org.scale7.cassyndex;

import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

public abstract class KeyIndexBase {

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

	/**
	 * Reports whether a search can be made on the provided key index given the configuration of the index.
	 * The minimum key prefix length is determined by the size of the key prefix taken to hash to key range buckets.
	 * So for example, if your bucket length key prefix is 3, then you can only search on key prefixes of length 3
	 * or greater.
	 * @param keyPrefix
	 * @return
	 */
	public boolean isValidKeyPrefix(String keyPrefix) {
		if (keyPrefix.length() >= config.bucketKeyPrefixLen)
			return true;
		return false;
	}

	protected String pelopsPool;
	protected Config config;
	protected Selector selector;

	protected KeyIndexBase(String pelopsPool, Config config) {
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
		if (!isValidKeyPrefix(keyPrefix))
			throw new Exception("This index only supports searching for keys with prefixes equal to or larger than: " + config.bucketKeyPrefixLen);
	}
}
