package org.gridkit.data.extractors.common;

import java.nio.ByteBuffer;

/**
 * <p>
 * {@link LazyFilterExtractor} always create a dedicated {@link BinaryExtractorSet}
 * for processor part of extractor.
 * </p>
 * <p>
 * This clould be useful to avoid parsing binary stream is filter condition was not meet.
 * </p>
 *  
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class LazyFilterExtractor<V> extends FilterExtractor<V> {
	
	private static final long serialVersionUID = 20130611L;

	public LazyFilterExtractor(BinaryExtractor<Boolean> predicate, BinaryExtractor<V> processor) {
		super(predicate, processor);
	}

	@Override
	protected BinaryExtractor<?> wrapProcessor(BinaryExtractor<?> processor) {
		return new LazyExtractor(processor);
	}

	@Override
	protected void processValue(ScalarResultReceiver output, Object value) {
		Lazy lazy = (Lazy) value;
		if (lazy.calculate()) {
			output.push(lazy.get());
		}
	}
	
	@Override
	public <VV> BinaryExtractor<VV> pushDown(BinaryExtractor<VV> nested) {
		return new LazyFilterExtractor<VV>(predicate, processor.pushDown(nested));
	}
	
	@Override
	public String toString() {
		return "F" + super.toString().substring(1);
	}



	private static class Lazy implements VectorResultReceiver {
		
		private final BinaryExtractorSet set;
		private final int id;
		private final ByteBuffer buffer;
		private boolean present;
		private Object value;
		
		
		public Lazy(BinaryExtractorSet set, int id, ByteBuffer buffer) {
			this.set = set;
			this.id = id;
			this.buffer = buffer;
		}

		public boolean calculate() {
			set.extractAll(buffer, this);
			return present;
		}

		public Object get() {
			return value;
		}

		@Override
		public void push(int id, Object part) {
			if (id == this.id) {
				present = true;
				value = part;
			}
			else {
				throw new IllegalArgumentException("No such argument #" + id);
			}
		}
	}
	
	@SuppressWarnings({"serial", "rawtypes"})
	private static class LazyExtractor extends AbstractValueTransformer<ByteBuffer, Lazy> {
		
		private final BinaryExtractorSet set;
		private final BinaryExtractor extractor;
		private final int id;
		
		public LazyExtractor(BinaryExtractor extractor) {
			this.extractor = extractor;
			this.set = extractor.newExtractorSet();
			this.id = set.addExtractor(extractor);
			this.set.compile();
		}

		@Override
		protected Lazy transform(ByteBuffer buffer) {
			return new Lazy(set, id, buffer);
		}
		
		public String toString() {
			return "LAZY[" + extractor + "]";
		}
	}
}
