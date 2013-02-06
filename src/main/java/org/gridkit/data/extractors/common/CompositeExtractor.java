package org.gridkit.data.extractors.common;

import java.util.List;

public interface CompositeExtractor<V> extends BinaryExtractor<V> {

	public List<BinaryExtractor<?>> getSubExtractors();
	
	public ValueComposer newComposer();
	
	interface ValueComposer extends ResultVectorReceiver {		
		public void compose(ResultVectorReceiver output, int outputIndex);		
	}	
}
