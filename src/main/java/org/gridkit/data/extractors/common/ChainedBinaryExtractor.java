package org.gridkit.data.extractors.common;

import java.nio.ByteBuffer;
import java.util.List;

public class ChainedBinaryExtractor<V> implements CompositeExtractor<V> {
	
	private final BinaryExtractor<ByteBuffer> outter;
	private final BinaryExtractor<V> inner;
	
	private ChainedBinaryExtractor(BinaryExtractor<ByteBuffer> outter, BinaryExtractor<V> inner) {
		this.outter = outter;
		this.inner = inner;
	}

	@Override
	public BinaryExtractorSet newExtractorSet() {
		return null;
	}

	@Override
	public boolean isCompatible(BinaryExtractorSet set) {
		return false;
	}

	@Override
	public List<BinaryExtractor<?>> getSubExtractors() {
		return null;
	}

	@Override
	public org.gridkit.data.extractors.common.CompositeExtractor.ValueComposer<V> newComposer() {
		return null;
	}
}
