package org.gridkit.data.extractors.common;

import java.io.Serializable;


public abstract class AbstractCompositeExtractor<V> implements CompositeExtractor<V>, Serializable {

	private static final long serialVersionUID = 20130205L;

	@Override
	public BinaryExtractorSet newExtractorSet() {
		return new CompositeExtractorSet();
	}

	@Override
	public boolean isCompatible(BinaryExtractorSet set) {
		return false;
	}

	@Override
	public boolean canPushDown(BinaryExtractor<?> nested) {
		return false;
	}

	@Override
	public <VV> BinaryExtractor<VV> pushDown(BinaryExtractor<VV> nested) {
		throw new IllegalArgumentException("Cannot push down");
	}

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object obj);

	@Override
	public abstract String toString();
}
