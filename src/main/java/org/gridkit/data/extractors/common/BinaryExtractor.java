package org.gridkit.data.extractors.common;


public interface BinaryExtractor<V> {
	
	public BinaryExtractorSet newExtractorSet();
	
	public boolean isCompatible(BinaryExtractorSet set);
	
}
