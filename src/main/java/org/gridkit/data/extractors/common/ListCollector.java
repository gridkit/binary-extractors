package org.gridkit.data.extractors.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListCollector<V> extends AbstractCompositeExtractor<List<V>> {

	private static final long serialVersionUID = 20130205L;
	
	public static <V> ListCollector<V> wrap(BinaryExtractor<V> extractor) {
		return new ListCollector<V>(extractor);
	}
	
	private final BinaryExtractor<V> itemExtractor;

	public ListCollector(BinaryExtractor<V> itemExtractor) {
		this.itemExtractor = itemExtractor;
	}

	@Override
	public List<BinaryExtractor<?>> getSubExtractors() {
		return Collections.<BinaryExtractor<?>>singletonList(itemExtractor);
	}

	@Override
	public ValueComposer newComposer() {
		return new ListComposer<V>();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((itemExtractor == null) ? 0 : itemExtractor.hashCode());
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
		ListCollector other = (ListCollector) obj;
		if (itemExtractor == null) {
			if (other.itemExtractor != null)
				return false;
		} else if (!itemExtractor.equals(other.itemExtractor))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "all(" + itemExtractor + ")";
	}

	private static class ListComposer<V> implements ValueComposer {
		
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
		public void compose(ScalarResultReceiver output) {
			output.push(list);			
		}
	}
}
