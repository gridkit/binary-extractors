package org.gridkit.data.extractors.common;


public interface BinaryExtractor<V> {
	
	public BinaryExtractorSet newExtractorSet();
	
	public boolean isCompatible(BinaryExtractorSet set);

	public boolean canPushDown(BinaryExtractor<?> nested);
	
	public <VV> BinaryExtractor<VV> pushDown(BinaryExtractor<VV> nested);
	
}
