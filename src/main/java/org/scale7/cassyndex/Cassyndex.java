package org.scale7.cassyndex;

public class Cassyndex {

	public static CaseSenKeyIndex createCsKeyOnlyIndex(String pelopsPool, KeyIndexBase.Config config) {
		return new CaseSenKeyIndex(pelopsPool, config);
	}

	public static CaseInsKeyIndex createCisKeyOnlyIndex(String pelopsPool, CaseInsKeyIndex.Config config) {
		return new CaseInsKeyIndex(pelopsPool, config);
	}
	
	public static FullTextIndex createFullTextIndex(String pelopsPool, FullTextIndex.Config config) {
		return new FullTextIndex(pelopsPool, config);
	}
}
