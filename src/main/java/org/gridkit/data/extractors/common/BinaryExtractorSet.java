package org.gridkit.data.extractors.common;

import java.nio.ByteBuffer;

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
	
	public void extractAll(ByteBuffer buffer, ResultVectorReceiver resultReceiver);

}
