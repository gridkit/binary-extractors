package org.gridkit.data.extractors.common;


public interface BinaryExtractorSet {
	
	/**
	 * Adds extractor to a array and return its ID 
	 * @param id
	 * @param extractor
	 * @return id of added extractor
	 */
	public int addExtractor(BinaryExtractor<?> extractor);

	public int getSize();
	
	public void compile();
	
	public void dump(StringBuilder builder);
	
	public void extractAll(Object source, VectorResultReceiver resultReceiver);

}
