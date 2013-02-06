package org.gridkit.data.extractors.common;

import java.nio.ByteBuffer;

/**
 * "Do nothing" extractor, it will just let input byte buffer pass through. 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public final class VerbatimExtractor implements BinaryExtractor<ByteBuffer>, PushDownBinaryExtractor {

	@Override
	public boolean canPushDown(BinaryExtractor<?> nested) {
		return true;
	}

	@Override
	public <V> BinaryExtractor<V> pushDown(BinaryExtractor<V> nested) {
		return nested;
	}

	@Override
	public BinaryExtractorSet newExtractorSet() {
		return new VerbatimExtractorSet(this);
	}

	@Override
	public boolean isCompatible(BinaryExtractorSet set) {
		return set instanceof VerbatimExtractorSet && ((VerbatimExtractorSet)set).getInstance() == this;
	}
	
	private static class VerbatimExtractorSet extends AbstractSingleExtractorSet {

		private VerbatimExtractorSet(BinaryExtractor<?> instance) {
			super(instance);
		}

		@Override
		protected Object extract(ByteBuffer buffer) {
			return buffer.slice();
		}
	}
}
