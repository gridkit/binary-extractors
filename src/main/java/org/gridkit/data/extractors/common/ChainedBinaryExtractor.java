package org.gridkit.data.extractors.common;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class ChainedBinaryExtractor<V> extends AbstractCompositeExtractor<V> {
	
	private static final long serialVersionUID = 20130205L;
	
	public static ChainedBinaryExtractor<ByteBuffer> chain() {
		return new ChainedBinaryExtractor<ByteBuffer>(new VerbatimExtractor(), new VerbatimExtractor());
	}
	
	public static <V> ChainedBinaryExtractor<V> chain(BinaryExtractor<ByteBuffer> outter, BinaryExtractor<V> inner) {
		return new ChainedBinaryExtractor<V>(outter, inner);
	}
	
	private final BinaryExtractor<ByteBuffer> outter;
	private final BinaryExtractor<V> inner;
	
	private ChainedBinaryExtractor(BinaryExtractor<ByteBuffer> outter, BinaryExtractor<V> inner) {
		this.outter = outter;
		this.inner = inner;
	}

	@SuppressWarnings("unchecked")
	public <VV> ChainedBinaryExtractor<VV> chain(BinaryExtractor<VV> tail) {
		if (inner.canPushDown(tail)) {
			BinaryExtractor<VV> ninner = inner.pushDown(tail);
			if (outter.canPushDown(ninner)) {
				return chain(VerbatimExtractor.INSTANCE, outter.pushDown(ninner));
			}
			else {
				return new ChainedBinaryExtractor<VV>(outter, ninner);
			}
		}
		else {
			return chain((ChainedBinaryExtractor<ByteBuffer>)this, tail);
		}
	}
	
	@Override
	public boolean canPushDown(BinaryExtractor<?> nested) {
		return inner.canPushDown(nested);
	}

	@Override
	public <VV> BinaryExtractor<VV> pushDown(BinaryExtractor<VV> nested) {
		BinaryExtractor<VV> ninner = inner.pushDown(nested);
		if (outter.canPushDown(ninner)) {
			return outter.pushDown(ninner);
		}
		else {
			return new ChainedBinaryExtractor<VV>(outter, ninner);
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public List<BinaryExtractor<?>> getSubExtractors() {
		if (outter.canPushDown(inner)) {
			return (List)Collections.singletonList(outter.pushDown(inner));
		}
		else {
			return (List)Collections.singletonList(outter);
		}
	}

	@Override
	public ValueComposer newComposer() {
		if (outter.canPushDown(inner)) {
			return new AsIsComposer();
		}
		else {
			return new ChainComposer(inner);
		}
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
		return outter + "/" + inner;
	}

	private static class ChainComposer extends AsIsComposer {

		private final BinaryExtractor<?> extractor;
		
		private ChainComposer(BinaryExtractor<?> extractor) {
			this.extractor = extractor;
		}

		@Override
		public void push(int id, Object part) {
			if (id == 0) {
				ByteBuffer binary = (ByteBuffer) part;
				super.push(0,Extractors.extract(binary, extractor));
			}
			else {
				throw new IllegalArgumentException("No such parameter: " + id);
			}
		}
	}
}
