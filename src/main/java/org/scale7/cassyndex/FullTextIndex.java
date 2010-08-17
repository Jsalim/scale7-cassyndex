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
import org.apache.cassandra.thrift.NotFoundException;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

public class FullTextIndex extends IndexBase {

	final Logger logger = SystemProxy.getLoggerFromFactory(FullTextIndex.class);

	protected CisKeyOnlyIndex termIndex;

	protected final static String ORIGINAL_TEXT_ID_PREFIX = "___";
	protected final static String ORIGINAL_TEXT_ID_COLUMN = "OriginalText";
	protected final static String WORD_PARSER_REGULAR_EXP = "[,\\s]";
	protected final static char TERM_TO_ENTRYID_SEPARATOR = (char)(Character.MAX_VALUE-1); // needs to be 1 less than max so appear in key prefix results

	public static class Config extends IndexBase.Config {

		protected String[] blockWords;

		public Config(String idxColumnFamily, String[] blockWords) {
			this(idxColumnFamily, 2, blockWords);
		}

		public Config(String idxColumnFamily, int bucketKeyPrefixLen, String[] blockWords) {
			super(idxColumnFamily, bucketKeyPrefixLen);
			this.blockWords = blockWords;
			for (int i=0; i<this.blockWords.length; i++) {
				this.blockWords[i] = normalizeWord(this.blockWords[i]);
			}
		}

		protected boolean isBlockWord(String word) {
			if (Arrays.binarySearch(blockWords, normalizeWord(word)) >=0)
				return true;
			return false;
		}
	}

	protected FullTextIndex(String pelopsPool, Config config) {
		super(pelopsPool, config);

		termIndex = new CisKeyOnlyIndex(pelopsPool, config);
	}

	public void addItem(String itemId, String itemText, ConsistencyLevel cLevel) throws Exception {
		VALIDATE_ITEM_ID(itemId);

		// Clean out existing item before writing new words, otherwise create superset
		removeItem(itemId, itemText, cLevel);

		// Store original copy of item text. We need this to remove the item later, and also to be able upgrade
		// the indexing algorithm
		Mutator mutator = Pelops.createMutator(pelopsPool);
		mutator.writeColumn(config.idxColumnFamily, getOriginalItemRecordId(itemId), mutator.newColumn(ORIGINAL_TEXT_ID_COLUMN, itemText));
		mutator.execute(cLevel);

		// Add reverse index lookup entries
		Iterable<String> words = Splitter
		.onPattern(WORD_PARSER_REGULAR_EXP)
		.trimResults()
		.omitEmptyStrings()
		.split(itemText);
		for (String word : words) {
			// Prepare word
			word = normalizeWord(word);
			// Ignore block words
			if (((Config)config).isBlockWord(word))
				continue;
			// Write compound key
			String key = createWordToItemIdCompoundKey(word, itemId);
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
			column = selector.getColumnFromRow(config.idxColumnFamily, getOriginalItemRecordId(itemId), ORIGINAL_TEXT_ID_COLUMN, ConsistencyLevel.QUORUM);
		} catch (NotFoundException ex) {
			// If does not exist, then vacuously succeed
			return;
		}
		String itemText = Selector.getColumnStringValue(column);

		// Check we need to remove this item
		if (itemText.equals(unlessHasText))
			return;

		// Remove reverse index lookup entries
		Iterable<String> words = Splitter
		.onPattern(WORD_PARSER_REGULAR_EXP)
		.trimResults()
		.omitEmptyStrings()
		.split(itemText);
		for (String word : words) {
			// Prepare word
			word = normalizeWord(word);
			// Do not ignore block words. Cannot trust that a programmer didn't accidentally change list!
			//if (((Config)config).isBlockWord(word))
				//continue;
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
		Iterable<String> terms = Splitter
		.onPattern(WORD_PARSER_REGULAR_EXP)
		.trimResults()
		.omitEmptyStrings()
		.split(searchText);

		// Count matches
		int termCount = 0; // we need this later
		HashMap<String, ItemMatchCount> matchStrength = new HashMap<String, ItemMatchCount>();
		LinkedList<ItemMatchCount> matchesList = new LinkedList<ItemMatchCount>();
		for (String term: terms) {
			// Check allowed term
			if (((Config)config).isBlockWord(term))
				continue;
			// Increment count of required terms
			termCount++;
			// Prepare index key prefix
			term = normalizeWord(term);
			// Iterate through all matching word keys
			IKeyIterator w = termIndex.getIterator(term, false, 1000, cLevel);
			while (w.hasNext()) {
				String[] wordToIdKeys = w.next();
				// Process word key page retrieved from Cassandra
				for (String wordToIdKey: wordToIdKeys) {
					// Split word key into word and term id components
					int divider = wordToIdKey.indexOf(TERM_TO_ENTRYID_SEPARATOR);
					if (divider != -1) {
						String word = wordToIdKey.substring(0, divider);
						String itemId = wordToIdKey.substring(divider+1);
						if (word.length() > 0 && itemId.length() > 0) {
							// Increment match count for item
							ItemMatchCount imc = matchStrength.get(itemId);
							if (imc == null) {
								imc = new ItemMatchCount(itemId);
								matchStrength.put(itemId, imc);
								matchesList.add(imc);
							}
							imc.hits++;
							if (term.length() == word.length())
								// Exact match on term, rather than just prefix, counts double!
								imc.strength += 2;
							else
								imc.strength++;
						}
					}
				}
			}
		}

		// Prune results that don't match all search terms
		Iterator<ItemMatchCount> i = matchesList.iterator();
		while (i.hasNext()) {
			ItemMatchCount imc = i.next();
			if (imc.hits < termCount)
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
				Column column = selector.getColumnFromRow(config.idxColumnFamily, getOriginalItemRecordId(itemIds[i]), ORIGINAL_TEXT_ID_COLUMN, cLevel);
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
		int strength = 0;
		int hits = 0;
	};

	private class DecreasingMatchStrength implements Comparator<ItemMatchCount> {

		@Override
		public int compare(ItemMatchCount o1, ItemMatchCount o2) {
			if (o1.strength > o2.strength)
				return -1;
			else if (o1.strength == o2.strength)
				return 0;
			else
				return 1;
		}
	};

	protected static String getOriginalItemRecordId(String itemId) {
		return ORIGINAL_TEXT_ID_PREFIX + itemId;
	}

	protected static String createWordToItemIdCompoundKey(String word, String itemId) {
		return word + TERM_TO_ENTRYID_SEPARATOR + itemId;
	}

	protected static String normalizeWord(String word) {
		return word.toLowerCase();
	}
}
