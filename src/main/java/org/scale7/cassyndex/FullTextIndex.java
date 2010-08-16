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

import com.google.common.base.Splitter;

public class FullTextIndex extends IndexBase {
	
	protected CisKeyOnlyIndex termIndex;

	protected final static String ORIGINAL_TEXT_ID_PREFIX = "___";
	protected final static String ORIGINAL_TEXT_ID_COLUMN = "OriginalText";

	public static class Config extends IndexBase.Config {

		protected String[] blockWords;

		public Config(String idxColumnFamily, String[] blockWords) {
			this(idxColumnFamily, 2, blockWords);
		}

		public Config(String idxColumnFamily, int bucketKeyPrefixLen, String[] blockWords) {
			super(idxColumnFamily, bucketKeyPrefixLen);
			this.blockWords = blockWords;
		}

		protected boolean isBlockWord(String word) {
			word = word.toLowerCase();
			if (Arrays.binarySearch(blockWords, word) != -1)
				return true;
			return false;
		}
	}

	protected FullTextIndex(String pelopsPool, Config config) {
		super(pelopsPool, config);

		termIndex = new CisKeyOnlyIndex(pelopsPool, config);
	}

	public void addItem(String itemId, String itemText, ConsistencyLevel cLevel) throws Exception {
		// Clean out existing item before writing new words, otherwise create superset
		removeItem(itemId, cLevel);

		// Store original copy of item text. We need this to remove the item later, and also to be able upgrade
		// the indexing algorithm
		Mutator mutator = Pelops.createMutator(pelopsPool);
		mutator.writeColumn(config.idxColumnFamily, getOriginalItemRecordId(itemId), mutator.newColumn(ORIGINAL_TEXT_ID_COLUMN, itemText));
		mutator.execute(cLevel);

		// Add reverse index lookup entries
		Iterable<String> words = Splitter
		.onPattern(",\\s*")
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
		// Load original text
		Column column;
		try {
			column = selector.getColumnFromRow(config.idxColumnFamily, getOriginalItemRecordId(itemId), ORIGINAL_TEXT_ID_COLUMN, cLevel);
		} catch (NotFoundException ex) {
			// If does not exist, then vacuously succeed
			return;
		}
		String itemText = Selector.getColumnStringValue(column);

		// Remove reverse index lookup entries
		Iterable<String> words = Splitter
		.onPattern(",\\s*")
		.trimResults()
		.omitEmptyStrings()
		.split(itemText);
		for (String word : words) {
			// Prepare word
			word = normalizeWord(word);
			// Ignore block words
			if (((Config)config).isBlockWord(word))
				continue;
			// Remove reverse entry lookup
			String key = createWordToItemIdCompoundKey(word, itemId);
			try {
				termIndex.deleteKey(key, cLevel);
			} catch (NotFoundException ex) {
				// If does not exist, then vacuously succeed
				return;
			}
		}

		// Remove record of item
		RowDeletor rowDeletor = Pelops.createRowDeletor(pelopsPool);
		rowDeletor.deleteRow(config.idxColumnFamily, getOriginalItemRecordId(itemId), cLevel);
	}

	public String[] findItems(String searchText, int maxResults, ConsistencyLevel cLevel) throws Exception {
		// Extract search terms
		Iterable<String> terms = Splitter
		.onPattern(",\\s*")
		.trimResults()
		.omitEmptyStrings()
		.split(searchText);

		// Count matches
		int termCount = 0; // we need this later
		HashMap<String, ItemMatchCount> matchStrength = new HashMap<String, ItemMatchCount>();
		LinkedList<ItemMatchCount> matchesList = new LinkedList<ItemMatchCount>();
		for (String term: terms) {
			termCount++;
			// Get normalized term
			term = normalizeWord(term);
			// Iterate through all matching word keys
			IKeyIterator w = termIndex.getIterator(term, false, 1000, cLevel);
			while (w.hasNext()) {
				String[] wordToIdKeys = w.next();
				// Process word key page retrieved from Cassandra
				for (String wordToIdKey: wordToIdKeys) {
					// Split word key into word and term id components
					int divider = wordToIdKey.indexOf(Character.MAX_VALUE);
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
		Collections.sort(matchesList, new ResultsSorter());

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
	
	private static class ItemMatchCount {
		public ItemMatchCount(String itemId) {
			this.itemId = itemId;
		}
		String itemId;
		int strength = 0;
		int hits = 0;
	};

	private class ResultsSorter implements Comparator<ItemMatchCount> {

		@Override
		public int compare(ItemMatchCount o1, ItemMatchCount o2) {
			if (o1.strength > o2.strength)
				return 1;
			else if (o1.strength == o2.strength)
				return 0;
			else
				return -1;
		}
	};

	protected static String getOriginalItemRecordId(String itemId) {
		return ORIGINAL_TEXT_ID_PREFIX + itemId;
	}

	protected static String createWordToItemIdCompoundKey(String word, String itemId) {
		return word + Character.MAX_VALUE + itemId;
	}

	protected static String normalizeWord(String word) {
		return word.toLowerCase();
	}
}
