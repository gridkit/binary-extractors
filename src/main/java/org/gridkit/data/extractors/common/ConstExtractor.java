package org.gridkit.data.extractors.common;

import java.util.Collections;
import java.util.List;

public class ConstExtractor<V> implements CompositeExtractor<V> {

	public static <V> ConstExtractor<V> newConst(V value) {
		return new ConstExtractor<V>(value);
	}
	
	private final V value;
	
	public ConstExtractor(V value) {
		this.value = value;
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
		return Collections.emptyList();
	}

	@Override
	public ValueComposer<V> newComposer() {
		return new ValueComposer<V>() {

			@Override
			public void push(int id, Object part) {
				throw new IllegalArgumentException("No such parameter");
			}

			@Override
			public void compose(ResultVectorReceiver output, int outputIndex) {
				output.push(outputIndex, value);
				
			}
		};
	}
}
