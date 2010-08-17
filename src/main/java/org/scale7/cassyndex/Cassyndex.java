package org.scale7.cassyndex;

public class Cassyndex {

	public static CsKeyOnlyIndex createCsKeyOnlyIndex(String pelopsPool, IndexBase.Config config) {
		return new CsKeyOnlyIndex(pelopsPool, config);
	}

	public static CisKeyOnlyIndex createCisKeyOnlyIndex(String pelopsPool, IndexBase.Config config) {
		return new CisKeyOnlyIndex(pelopsPool, config);
	}
	
	public static FullTextIndex createFullTextIndex(String pelopsPool, FullTextIndex.Config config) {
		return new FullTextIndex(pelopsPool, config);
	}
}
