package org.gridkit.data.extractors.common;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ChainedBinaryExtractor<V> implements BinaryExtractor<V>, ExtractorChain, Serializable {
	
	private static final long serialVersionUID = 20130205L;
	
	public static ChainedBinaryExtractor<ByteBuffer> chain() {
		return new ChainedBinaryExtractor<ByteBuffer>(new VerbatimExtractor(), new VerbatimExtractor());
	}
	
	public static <V> ChainedBinaryExtractor<V> chain(BinaryExtractor<?> outter, BinaryExtractor<V> inner) {
		if (outter.canPushDown(inner)) {
			return new ChainedBinaryExtractor<V>(outter.pushDown(inner), null);
		}
		else {
			return new ChainedBinaryExtractor<V>(outter, inner);
		}
	}
	
	private final BinaryExtractor<?> outter;
	private final BinaryExtractor<V> inner;
	
	private ChainedBinaryExtractor(BinaryExtractor<?> outter, BinaryExtractor<V> inner) {
		this.outter = outter;
		this.inner = inner;
	}

	public <VV> ChainedBinaryExtractor<VV> chain(BinaryExtractor<VV> tail) {
		return chain(this, tail);
	}

	@Override
	public BinaryExtractorSet newExtractorSet() {
		return new CompositeExtractorSet();
	}

	@Override
	public boolean isCompatible(BinaryExtractorSet set) {
		return set instanceof CompositeExtractorSet;
	}

	@Override
	public boolean canPushDown(BinaryExtractor<?> nested) {
		if (inner == null) {
			return true;
		}
		else {
			return inner.canPushDown(nested);
		}
	}

	@Override
	public <VV> BinaryExtractor<VV> pushDown(BinaryExtractor<VV> nested) {
		if (inner == null) {
			if (outter.canPushDown(nested)) {
				return new ChainedBinaryExtractor<VV>(outter.pushDown(nested), null);
			}
			else {
				return new ChainedBinaryExtractor<VV>(outter, nested);
			}
		}
		else {
			return new ChainedBinaryExtractor<VV>(outter, inner.pushDown(nested));
		}
	}

	@Override
	public BinaryExtractor<?> getHead() {
		return outter;
	}

	@Override
	public BinaryExtractor<?> getTail() {
		return inner;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((inner == null) ? 0 : inner.hashCode());
		result = prime * result + ((outter == null) ? 0 : outter.hashCode());
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
		ChainedBinaryExtractor other = (ChainedBinaryExtractor) obj;
		if (inner == null) {
			if (other.inner != null)
				return false;
		} else if (!inner.equals(other.inner))
			return false;
		if (outter == null) {
			if (other.outter != null)
				return false;
		} else if (!outter.equals(other.outter))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return outter + (inner != null ? ("/" + inner) : "");
	}
}
