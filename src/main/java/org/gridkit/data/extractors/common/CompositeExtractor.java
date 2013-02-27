package org.gridkit.data.extractors.common;

import java.util.List;

public interface CompositeExtractor<V> extends BinaryExtractor<V> {

	public List<BinaryExtractor<?>> getSubExtractors();
	
	public ValueComposer newComposer();
	
	interface ValueComposer extends VectorResultReceiver {		
		public void compose(ScalarResultReceiver receiver);		
	}	
}
