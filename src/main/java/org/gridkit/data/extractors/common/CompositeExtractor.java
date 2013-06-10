package org.gridkit.data.extractors.common;

import java.util.ArrayList;
import java.util.List;

public interface CompositeExtractor<V> extends BinaryExtractor<V> {

	public List<BinaryExtractor<?>> getSubExtractors();
	
	public ValueComposer newComposer();
	
	interface ValueComposer extends VectorResultReceiver {		
		public void compose(ScalarResultReceiver receiver);		
	}	
		
	public static abstract class SingleArgumentComposer implements ValueComposer {

		protected List<Object> inputs = new ArrayList<Object>(1);
		
		@Override
		public void push(int id, Object part) {
			if (id == 0) {
				inputs.add(part);
			}
			else {
				throw new IllegalArgumentException("No such input slot #" + id);
			}
		}

		@Override
		public void compose(ScalarResultReceiver receiver) {
			for(Object input: inputs) {
				processInput(input, receiver);
			}
		}

		protected abstract void processInput(Object input, ScalarResultReceiver receiver);
	}
}
