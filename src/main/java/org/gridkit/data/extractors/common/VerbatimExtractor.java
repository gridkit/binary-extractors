package org.gridkit.data.extractors.common;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * "Do nothing" extractor, it will just let input byte buffer pass through. 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public final class VerbatimExtractor implements BinaryExtractor<ByteBuffer>, Serializable {

	private static final long serialVersionUID = 20130205L;

	public static final VerbatimExtractor INSTANCE = new VerbatimExtractor();
	
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
	
	@Override
	public int hashCode() {
		return getClass().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "VERBATIM";
	}

	private static class VerbatimExtractorSet extends AbstractSingleExtractorSet {

		private VerbatimExtractorSet(BinaryExtractor<?> instance) {
			super(instance);
		}

		@Override
		protected Object extract(ByteBuffer buffer) {
			return buffer.slice();
		}

		@Override
		public void dump(StringBuilder builder) {
			builder.append("<verbatim/>\n");
		}
	}
}
