package org.gridkit.data.extractors.common;

import java.util.Arrays;
import java.util.List;

public class BinaryFilterExtractor<V> extends AbstractCompositeExtractor<V> {
	
	private final BinaryExtractor<Boolean> predicate;
	private final BinaryExtractor<V> processor;

	public BinaryFilterExtractor(BinaryExtractor<Boolean> predicate, BinaryExtractor<V> processor) {
		this.predicate = predicate;
		this.processor = processor;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<BinaryExtractor<?>> getSubExtractors() {
		return Arrays.asList(predicate, processor);
	}

	@Override
	public ValueComposer<V> newComposer() {
		return new FilterComposer<V>();
	}
	
	private static class FilterComposer<V> implements ValueComposer<V> {
		
		private boolean passed;
		private boolean exists;
		private V value;

		@Override
		@SuppressWarnings("unchecked")
		public void push(int id, Object part) {
			if (id == 0) {
				passed = ((Boolean)part).booleanValue();
			}
			else if (id == 1) {
				exists = true;
				value = (V)part;
			}
			else {
				throw new IllegalArgumentException("No such parameter: " + id);
			}
		}

		@Override
		public void compose(ResultVectorReceiver output, int outputIndex) {
			if (exists && passed) {
				output.push(outputIndex, value);
			}			
		}
	}
}
