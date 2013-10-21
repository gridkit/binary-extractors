package org.gridkit.data.extractors.common;

import java.util.Map;

/**
 * Simple extractor extracting specific key from {@link Map}.
 * This could be useful if you want access composite data structure (e.g. binary blob + metadata) in expression graph.
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 *
 * @param <In>
 * @param <Out>
 */
public class SimpleMapExtractor<In extends Map<?, Out>, Out> extends AbstractValueTransformer<In, Out> {

	private static final long serialVersionUID = 20131021L;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> SimpleMapExtractor<Map<?, T>, T> extract(BinaryExtractor<? extends Map<?, ?>> source, Object key) {
		return new SimpleMapExtractor(source, key);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> SimpleMapExtractor<Map<?, T>, T> extract(Object key) {
		return new SimpleMapExtractor(VerbatimExtractor.INSTANCE, key);
	}
	
	private Object key;
	
	public SimpleMapExtractor() {
		super(null);
	}

	public SimpleMapExtractor(BinaryExtractor<In> sourceExtractor, Object key) {
		super(sourceExtractor);
		this.key = key;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		SimpleMapExtractor other = (SimpleMapExtractor) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	protected Out transform(In input) {
		return input.get(key);
	}

	@Override
	protected String getDescription() {
		return "GET[" + key + "]";
	}
}
