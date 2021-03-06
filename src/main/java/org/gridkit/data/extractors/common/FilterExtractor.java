package org.gridkit.data.extractors.common;

import java.util.Arrays;
import java.util.List;

public class FilterExtractor<V> extends AbstractCompositeExtractor<V> {
	
	private static final long serialVersionUID = 20130205L;
	
	protected final BinaryExtractor<Boolean> predicate;
	protected final BinaryExtractor<V> processor;

	public static <T> FilterExtractor<T> filter(BinaryExtractor<Boolean> prediacte, BinaryExtractor<T> processor) {
		return new FilterExtractor<T>(prediacte, processor);
	}

	public static <V> FilterExtractor<V> lazyFilter(BinaryExtractor<Boolean> prediacte, BinaryExtractor<V> processor) {
		return new LazyFilterExtractor<V>(prediacte, processor);
	}
	
	public FilterExtractor(BinaryExtractor<Boolean> predicate, BinaryExtractor<V> processor) {
		this.predicate = predicate;
		this.processor = processor;
	}

	@Override
	public Object getOperationToken() {
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<BinaryExtractor<?>> getSubExtractors() {
		return Arrays.asList(predicate, wrapProcessor(processor));
	}

	protected BinaryExtractor<?> wrapProcessor(BinaryExtractor<?> processor) {
		return processor;
	}
	
	@Override
	public boolean canPushDown(BinaryExtractor<?> nested) {		
		return processor.canPushDown(nested);
	}

	@Override
	public <VV> BinaryExtractor<VV> pushDown(BinaryExtractor<VV> nested) {
		return new FilterExtractor<VV>(predicate, processor.pushDown(nested));
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
		FilterExtractor other = (FilterExtractor) obj;
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

	protected void processValue(ScalarResultReceiver output, Object value) {
		output.push(value);
	}
	
	@Override
	public String toString() {
		return "f(" + predicate + ")/" + processor;
	}

	private class FilterComposer implements ValueComposer {
		
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
				processValue(output, value);
			}			
		}
	}
}
