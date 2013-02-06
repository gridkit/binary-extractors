package org.gridkit.data.extractors.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListCollector<V> implements CompositeExtractor<List<V>> {

	private final BinaryExtractor<V> itemExtractor;

	public ListCollector(BinaryExtractor<V> itemExtractor) {
		this.itemExtractor = itemExtractor;
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
	public List<BinaryExtractor<?>> getSubExtractors() {
		return Collections.<BinaryExtractor<?>>singletonList(itemExtractor);
	}

	@Override
	public ValueComposer<List<V>> newComposer() {
		return new ListComposer<V>();
	}
	
	private static class ListComposer<V> implements ValueComposer<List<V>> {
		
		private List<V> list = new ArrayList<V>();

		@Override
		@SuppressWarnings("unchecked")
		public void push(int id, Object part) {
			if (id != 0) {
				throw new IllegalArgumentException("Invalid argument index: " + id);
			}
			list.add((V)part);
		}

		@Override
		public void compose(ResultVectorReceiver output, int outputIndex) {
			output.push(outputIndex, list);			
		}
	}
}
