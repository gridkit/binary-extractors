package org.gridkit.data.extractors.common;


public abstract class AbstractCompositeExtractor<V> implements CompositeExtractor<V> {

	@Override
	public BinaryExtractorSet newExtractorSet() {
		return new CompositeExtractorSet();
	}

	@Override
	public boolean isCompatible(BinaryExtractorSet set) {
		return false;
	}
}
