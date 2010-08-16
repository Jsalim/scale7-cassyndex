package org.scale7.cassyndex;

public interface IKeyIterator {

	boolean hasNext() throws Exception;

	String[] next() throws Exception;
};