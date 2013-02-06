package org.gridkit.data.extractors.common;

public interface PushDownBinaryExtractor {

	public boolean canPushDown(BinaryExtractor<?> nested);
	
	public <V> BinaryExtractor<V> pushDown(BinaryExtractor<V> nested);
	
}
