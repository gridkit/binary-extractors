package org.gridkit.data.extractors.common;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Forces extractor to use separate {@link BinaryExtractorSet}.
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class HeavyExtractorWrapper<V> implements BinaryExtractor<V>, Serializable {

	private static final long serialVersionUID = 20130611L;
	
	private final BinaryExtractor<V> extractor;

	public HeavyExtractorWrapper(BinaryExtractor<V> extractor) {
		this.extractor = extractor;
	}

	@Override
	public BinaryExtractorSet newExtractorSet() {
		return new DedicatedExtractorSet();
	}

	@Override
	public boolean isCompatible(BinaryExtractorSet set) {
		return false;
	}

	@Override
	public boolean canPushDown(BinaryExtractor<?> nested) {
		return extractor.canPushDown(nested);
	}

	@Override
	public <VV> BinaryExtractor<VV> pushDown(BinaryExtractor<VV> nested) {
		return new HeavyExtractorWrapper<VV>(extractor.pushDown(nested));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((extractor == null) ? 0 : extractor.hashCode());
		return result;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HeavyExtractorWrapper other = (HeavyExtractorWrapper) obj;
		if (extractor == null) {
			if (other.extractor != null)
				return false;
		} else if (!extractor.equals(other.extractor))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "HEAVY:" + extractor.toString();
	}
	
	private static class DedicatedExtractorSet implements BinaryExtractorSet {

		private BinaryExtractorSet delegate;
		
		@Override
		public int addExtractor(BinaryExtractor<?> extractor) {
			if (delegate == null) {
				delegate = extractor.newExtractorSet();
				return delegate.addExtractor(extractor);
			}
			else {
				throw new IllegalStateException("This extractor set is limited to one instance");
			}
		}

		@Override
		public int getSize() {
			return delegate == null ? 0 : delegate.getSize();
		}

		@Override
		public void compile() {
			delegate.compile();
		}

		@Override
		public void dump(StringBuilder builder) {
			builder.append("<heavy>\n");
			delegate.dump(builder);
			builder.append("\n</heavy>");
		}

		@Override
		public void extractAll(ByteBuffer buffer, VectorResultReceiver resultReceiver) {
			delegate.extractAll(buffer, resultReceiver);
		}
	}
}
