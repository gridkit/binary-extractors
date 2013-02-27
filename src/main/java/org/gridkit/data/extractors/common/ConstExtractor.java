package org.gridkit.data.extractors.common;

import java.util.Collections;
import java.util.List;

public class ConstExtractor<V> extends AbstractCompositeExtractor<V> {

	private static final long serialVersionUID = 20130205L;
	
	public static <V> ConstExtractor<V> newConst(V value) {
		return new ConstExtractor<V>(value);
	}
	
	private final V value;
	
	public ConstExtractor(V value) {
		this.value = value;
	}
	
	@Override
	public List<BinaryExtractor<?>> getSubExtractors() {
		return Collections.emptyList();
	}

	@Override
	public ValueComposer newComposer() {
		return new ValueComposer() {

			@Override
			public void push(int id, Object part) {
				throw new IllegalArgumentException("No such parameter");
			}

			@Override
			public void compose(ScalarResultReceiver output) {
				output.push(value);
			}
		};
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return String.valueOf(value);
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
		ConstExtractor other = (ConstExtractor) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
}
