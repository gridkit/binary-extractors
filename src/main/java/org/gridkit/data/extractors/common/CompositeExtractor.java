package org.gridkit.data.extractors.common;

import java.util.List;

public interface CompositeExtractor<V> extends BinaryExtractor<V> {

	public List<BinaryExtractor<?>> getSubExtractors();
	
	public ValueComposer<V> newComposer();
	
	interface ValueComposer<V> extends ResultVectorReceiver {		
		public void compose(ResultVectorReceiver output, int outputIndex);		
	}	
}
