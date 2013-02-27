package org.gridkit.data.extractors.common;

import java.util.Arrays;
import java.util.List;

public class BinaryFilterExtractor<V> extends AbstractCompositeExtractor<V> {
	
	private static final long serialVersionUID = 20130205L;
	
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
	public boolean canPushDown(BinaryExtractor<?> nested) {		
		return processor.canPushDown(nested);
	}

	@Override
	public <VV> BinaryExtractor<VV> pushDown(BinaryExtractor<VV> nested) {
		return new BinaryFilterExtractor<VV>(predicate, processor.pushDown(nested));
	}

	@Override
	public ValueComposer newComposer() {
		return new FilterComposer();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((predicate == null) ? 0 : predicate.hashCode());
		result = prime * result
				+ ((processor == null) ? 0 : processor.hashCode());
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
		BinaryFilterExtractor other = (BinaryFilterExtractor) obj;
		if (predicate == null) {
			if (other.predicate != null)
				return false;
		} else if (!predicate.equals(other.predicate))
			return false;
		if (processor == null) {
			if (other.processor != null)
				return false;
		} else if (!processor.equals(other.processor))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "f(" + predicate + ")/" + processor;
	}

	private static class FilterComposer implements ValueComposer {
		
		private boolean passed;
		private boolean exists;
		private Object value;

		@Override
		public void push(int id, Object part) {
			if (id == 0) {
				passed = ((Boolean)part).booleanValue();
			}
			else if (id == 1) {
				exists = true;
				value = part;
			}
			else {
				throw new IllegalArgumentException("No such parameter: " + id);
			}
		}

		@Override
		public void compose(ScalarResultReceiver output) {
			if (exists && passed) {
				output.push(value);
			}			
		}
	}
}
