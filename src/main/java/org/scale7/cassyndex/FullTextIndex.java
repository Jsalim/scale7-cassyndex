package org.scale7.cassyndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.exceptions.NotFoundException;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

public class FullTextIndex extends KeyIndexBase {

	final Logger logger = SystemProxy.getLoggerFromFactory(FullTextIndex.class);

	protected boolean initialized = false;
	protected CaseInsKeyIndex termIndex;

	//protected final static String META_DATA_ROW_KEY_ID = "__Meta";
	//protected final static String META_INIT_MARKER_COLUMN_ID = "Initialized";
	//protected final static String META_BLOCK_WORDS_COLUMN_ID = "BlockWords";
	protected final static String ENTRY_META_DATA_KEY_PREFIX = "___";
	protected final static String ENTRY_META_ORIGINAL_TEXT_COL_ID = "OriginalText";
	protected final static char TERM_TO_ENTRYID_SEPARATOR = (char)(Character.MAX_VALUE-1); // needs to be 1 less than max so appear in key prefix results

	public static class Config extends KeyIndexBase.Config {

		protected String[] blockWords;
		protected int matchTermPageSize = 2000;

		public Config(String idxColumnFamily) {
			this(idxColumnFamily, 2);
		}

		public Config(String idxColumnFamily, int bucketKeyPrefixLen) {
			super(idxColumnFamily, bucketKeyPrefixLen);
		}

		/**
		 * Set the block words used with the index. If an item is being added to the index, the block words
		 * inside the item text are not added. If a search is being performed against the index, the blocked
		 * words are not searched for. NOTE it is essential that the same set of block words is used when
		 * writing entries to the index and querying the index, otherwise unexpected search results may result.
		 * @param blockWords A list of block words that should be ignored
		 */
		public void setBlockWords(String[] blockWords) {
			this.blockWords = blockWords;
			for (int i=0; i<this.blockWords.length; i++) {
				this.blockWords[i] = normalizeWord(this.blockWords[i]);
			}
			Arrays.sort(this.blockWords);
		}

		protected boolean isBlockWordPrefix(String word) {
			if (blockWords != null)
				for (String blockWord : blockWords) {
					if (blockWord.startsWith(normalizeWord(word)))
						return true;
				}
			return false;
		}

		/**
		 * Set the max size of the page of matching terms retrieved from Cassandra when iterating through
		 * all term matches. This controls the maximum size of a read in bytes. For example, if you are using
		 * 80 byte stringified uuids a max page size of 2000 means you could pull at least 160K from one
		 * node per page retrieval operation. Note that reducing page size increases chance of balancing
		 * retrieval across replicas while also increasing round trips.
		 * @param matchTermPageSize
		 */
		public void setMatchTermPageSize(int matchTermPageSize) {
			this.matchTermPageSize = matchTermPageSize;
		}
	}

	protected FullTextIndex(String pelopsPool, Config config) {
		super(pelopsPool, config);

		CaseInsKeyIndex.Config cisConfig = new CaseInsKeyIndex.Config(config.idxColumnFamily, config.bucketKeyPrefixLen);
		cisConfig.setFullCaseKeys(false);
		termIndex = new CaseInsKeyIndex(pelopsPool, cisConfig);

		/*
		List<Column> columns;
		try {
			columns = selector.getColumnsFromRow(config.idxColumnFamily, META_DATA_ROW_KEY_ID, selector.newColumnsPredicateAll(false, 100), ConsistencyLevel.QUORUM);
			initialized = true;
			boolean foundBlockWords = false;
			for (Column column : columns) {
				// Check that block words match configuration
				if (Selector.getColumnStringValue(column).equals(META_BLOCK_WORDS_COLUMN_ID)) {
					String[] blockWords = getNormalizedWordsFromText(Selector.getColumnStringValue(column));
					Arrays.sort(blockWords);
					boolean matchesConfig = false;
					if (blockWords.length == config.blockWords.length) {
						int i = 0;
						for (; i<blockWords.length; i++)
							if (!blockWords[i].equals(config.blockWords[i]))
								break;
						matchesConfig = i == blockWords.length;
					}
				}
			}
		} catch (NotFoundException ex) {
			initialized = false;
		} */
	}

	public void addItem(String itemId, String itemText, ConsistencyLevel cLevel) throws Exception {
		VALIDATE_ITEM_ID(itemId);

		// Clean out existing item before writing new words, otherwise create superset
		removeItem(itemId, itemText, cLevel);

		// Store original copy of item text. We need this to remove the item later, and also to be able upgrade
		// the indexing algorithm
		Mutator mutator = Pelops.createMutator(pelopsPool);
		mutator.writeColumn(config.idxColumnFamily, getOriginalItemRecordId(itemId), mutator.newColumn(ENTRY_META_ORIGINAL_TEXT_COL_ID, itemText));
		mutator.execute(cLevel);

		// Add reverse index lookup entries
		String[] terms = getNormalizedSearchTermsFromText(itemText);
		for (String term : terms) {
			// Ignore block words
			if (((Config)config).isBlockWordPrefix(term))
				continue;
			// Write compound key
			String key = createWordToItemIdCompoundKey(term, itemId);
			termIndex.writeKey(key, cLevel);
		}
	}

	public void removeItem(String itemId, ConsistencyLevel cLevel) throws Exception {
		removeItem(itemId, null, cLevel);
	}

	protected void removeItem(String itemId, String unlessHasText, ConsistencyLevel cLevel) throws Exception {
		VALIDATE_ITEM_ID(itemId);

		// Load original text
		Column column;
		try {
			column = selector.getColumnFromRow(config.idxColumnFamily, getOriginalItemRecordId(itemId), ENTRY_META_ORIGINAL_TEXT_COL_ID, ConsistencyLevel.QUORUM);
		} catch (NotFoundException ex) {
			// If does not exist, then vacuously succeed
			return;
		}
		String itemText = Selector.getColumnStringValue(column);

		// Check we need to remove this item
		if (itemText.equals(unlessHasText))
			return;

		// Remove reverse index lookup entries
		String[] words = getNormalizedSearchTermsFromText(itemText);
		for (String word : words) {
			// !!! Do not ignore block words. Cannot trust that a programmer didn't accidentally change list!

			// Remove reverse entry lookup
			String key = createWordToItemIdCompoundKey(word, itemId);
			try {
				termIndex.deleteKey(key, cLevel);
			} catch (NotFoundException ex) {
				// If does not exist, keep going anyway. We must make sure entries don't exist.
			}
		}

		// Remove record of item
		RowDeletor rowDeletor = Pelops.createRowDeletor(pelopsPool);
		rowDeletor.deleteRow(config.idxColumnFamily, getOriginalItemRecordId(itemId), cLevel);
	}

	public String[] findItems(String searchText, int maxResults, ConsistencyLevel cLevel) throws Exception {
		// Extract search terms
		String[] terms = getNormalizedSearchTermsFromText(searchText);

		// Count matches
		HashMap<String, ItemMatchCount> matchStrengths = new HashMap<String, ItemMatchCount>();
		LinkedList<ItemMatchCount> matchesList = new LinkedList<ItemMatchCount>();
		for (int t=0; t<terms.length; t++) {
			String term = terms[t];
			if (termIndex.isValidKeyPrefix(term)) {
				// Iterate through all matching word keys
				IKeyIterator tokens = termIndex.getIterator(term, false, ((Config)config).matchTermPageSize, cLevel);
				while (tokens.hasNext()) {
					String[] tokenToIdKeys = tokens.next();
					// Process word key page retrieved from Cassandra
					for (String tokenToIdKey: tokenToIdKeys) {
						// Split word key into word and term id components
						int divider = tokenToIdKey.indexOf(TERM_TO_ENTRYID_SEPARATOR);
						if (divider != -1) {
							String token = tokenToIdKey.substring(0, divider);
							String itemId = tokenToIdKey.substring(divider+1);
							if (token.length() > 0 && itemId.length() > 0) {
								// Get match count for item
								ItemMatchCount imc = matchStrengths.get(itemId);
								if (imc == null) {
									imc = new ItemMatchCount(itemId);
									matchStrengths.put(itemId, imc);
									matchesList.add(imc);
								}
								// We only require/hit on single word search terms, not tuple terms
								if (isOneWordSearchTerm(term)) {
									// A term can only hit *once* e.g. mi against mike and michael does not equal two hits
									if (imc.lastMatchingWordIdx != t) {
										imc.wordTermHits++;
										imc.lastMatchingWordIdx = t;
										imc.lastMatchingWordStrength = getTermMatchStrength(token, term);
										imc.totalMatchStrength += imc.lastMatchingWordStrength;
									} else {
										// we don't double count, but take the strongest match
										int matchStrength = getTermMatchStrength(token, term);
										if (matchStrength > imc.lastMatchingWordStrength) {
											imc.totalMatchStrength -= imc.lastMatchingWordStrength;
											imc.totalMatchStrength += matchStrength;
											imc.lastMatchingWordStrength = matchStrength;
										}
									}
								} else {
									// A tuple can only match once as with a term
									if (imc.lastMatchingTupleIdx != t) {
										imc.lastMatchingTupleIdx = t;
										imc.lastMatchingTupleStrength = getTupleTermMatchStrength(token, term);
										imc.totalMatchStrength += imc.lastMatchingTupleStrength;
									} else {
										// we don't double count, but take the strongest match
										int matchStrength = getTupleTermMatchStrength(token, term);
										if (matchStrength > imc.lastMatchingTupleStrength) {
											imc.totalMatchStrength -= imc.lastMatchingTupleStrength;
											imc.totalMatchStrength += matchStrength;
											imc.lastMatchingTupleStrength = matchStrength;
										}
									}
								}
							}
						}
					}
				}
			}
		}

		// Count single word terms in search text
		int wordTermCount = 0;
		for (String term : terms)
			if (termIndex.isValidKeyPrefix(term) && isOneWordSearchTerm(term))
				wordTermCount++;

		// Prune items from results that haven't matched all single word terms
		Iterator<ItemMatchCount> i = matchesList.iterator();
		while (i.hasNext()) {
			ItemMatchCount imc = i.next();
			if (imc.wordTermHits < wordTermCount)
				i.remove();
		}

		// Sort results
		Collections.sort(matchesList, new DecreasingMatchStrength());

		// Return list of ids of matching items
		int itemCount = 0;
		List<String> results = new ArrayList<String>(matchesList.size());
		for (ItemMatchCount imc : matchesList) {
			if (++itemCount > maxResults)
				break;
			results.add(imc.itemId);
		}
		return results.toArray(new String[]{});
	}

	public static class TextTransform {
		boolean replaceLineBreaks = false;
		String lineBreakReplacement = " ";
		public TextTransform() {
		}
		public void setReplaceLineBreaks(boolean replaceLineBreaks) {
			this.replaceLineBreaks = replaceLineBreaks;
		}
		public void setLineBreakReplacement(String lineBreakReplacement) {
			this.lineBreakReplacement = lineBreakReplacement;
		}
	}

	public String[] itemIdsToText(String[] itemIds, int maxResults, ConsistencyLevel cLevel) throws Exception {
		return itemIdsToText(itemIds, maxResults, new TextTransform(), cLevel);
	}

	public String[] itemIdsToText(String[] itemIds, int maxResults, TextTransform textTransform, ConsistencyLevel cLevel) throws Exception {
		int resultsCount = Math.min(itemIds.length, maxResults);
		List<String> results = new ArrayList<String>(resultsCount);
		for (int i=0; i<resultsCount; i++) {
			String originalText;
			try {
				Column column = selector.getColumnFromRow(config.idxColumnFamily, getOriginalItemRecordId(itemIds[i]), ENTRY_META_ORIGINAL_TEXT_COL_ID, cLevel);
				originalText = Selector.getColumnStringValue(column);
				if (textTransform.replaceLineBreaks)
					originalText = CharMatcher.anyOf("\r\n").replaceFrom(originalText, textTransform.lineBreakReplacement);
			} catch (NotFoundException ex) {
				// If does not exist, then vacuously succeed
				originalText = "Error";
				logger.warn("Cannot retrieve original description for item: {}", itemIds[i]);
			}
			results.add(originalText);
		}
		return results.toArray(new String[] {});
	}

	private static int getTermMatchStrength(String token, String term) {
		if (token.length() == term.length())
			return 4;
		return 2;
	}

	private static int getTupleTermMatchStrength(String token, String tuple) {
		if (token.length() == tuple.length())
			return 2;
		return 1;
	}

	private void VALIDATE_ITEM_ID(String itemId) throws Exception {
		for (char c : itemId.toCharArray()) {
		    if (Character.isUpperCase(c)) {
		    	throw new Exception("Invalid item id: Uppercase letters may not be used in item identifiers.");
		    }
		    if (Character.isWhitespace(c)) {
		    	throw new Exception("Invalid item id: Whitespace may not be used inside item identifiers.");
		    }
		}
	}

	private static class ItemMatchCount {
		public ItemMatchCount(String itemId) {
			this.itemId = itemId;
		}
		String itemId;
		int wordTermHits = 0;
		int lastMatchingWordIdx = -1;
		int lastMatchingWordStrength = 0;
		int lastMatchingTupleIdx = -1;
		int lastMatchingTupleStrength = 0;
		int totalMatchStrength = 0;
	};

	private class DecreasingMatchStrength implements Comparator<ItemMatchCount> {

		@Override
		public int compare(ItemMatchCount o1, ItemMatchCount o2) {
			if (o1.totalMatchStrength > o2.totalMatchStrength)
				return -1;
			else if (o1.totalMatchStrength == o2.totalMatchStrength)
				return 0;
			else
				return 1;
		}
	};

	protected static String getOriginalItemRecordId(String itemId) {
		return ENTRY_META_DATA_KEY_PREFIX + itemId;
	}

	protected static String createWordToItemIdCompoundKey(String word, String itemId) {
		return word + TERM_TO_ENTRYID_SEPARATOR + itemId;
	}

	// Whether a term is a tuple term e.g. "exam results"
	protected boolean isOneWordSearchTerm(String term) {
		return !term.contains(" ");
	}

	// Extract search terms from text. Search terms include words and word tuples e.g "results" and "exam results"
	protected String[] getNormalizedSearchTermsFromText(String text) {
		String[] sentences = getSentencesFromText(text);
		List<String> result = new ArrayList<String>(250);
		for (String sentence : sentences) {
			String[] words = getNormalizedWordsFromSentence(sentence);
			String prevWord = null;
			for (String word : words) {
				if (!((Config)config).isBlockWordPrefix(word))
					result.add(word);
				if (prevWord != null) {
					String comboTerm = prevWord + " " + word;
					result.add(comboTerm);
				}
				prevWord = word;
			}
		}
		return result.toArray(new String[]{});
	}

	// Extract unbroken series of words e.g. sentences
	protected String[] getSentencesFromText(String text) {
		Iterable<String> sentences = Splitter
		.onPattern("[\\!\\(\\)\\{\\}\\=\\[\\]\\;\\:\\|\"\\?/\\<\\>\\.\\,]")
		.trimResults()
		.omitEmptyStrings()
		.split(text);
		List<String> result = new ArrayList<String>(250);
		for (String sentence : sentences)
			result.add(sentence);
		return result.toArray(new String[]{});
	}

	// Extract array of normalized word from series
	protected String[] getNormalizedWordsFromSentence(String sentence) {
		Iterable<String> words = Splitter
		.onPattern("[\\@\\£\\$\\%\\^\\&\\*\\+\\_\\~\\s]")
		.trimResults()
		.omitEmptyStrings()
		.split(sentence);
		List<String> result = new ArrayList<String>(100);
		for (String word : words) {
			if (!word.equals("-")) // double-barreled words are OK, hyphens are not
				result.add(normalizeWord(word));
		}
		return result.toArray(new String[]{});
	}

	protected static String normalizeWord(String word) {
		String normalized = CharMatcher.INVISIBLE.or(CharMatcher.anyOf("'")).removeFrom(word);
		normalized = normalized.toLowerCase();
		return normalized;
	}
}
