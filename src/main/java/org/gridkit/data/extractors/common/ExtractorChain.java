package org.gridkit.data.extractors.common;

public interface ExtractorChain {

	public BinaryExtractor<?> getHead();

	public BinaryExtractor<?> getTail();
	
}
