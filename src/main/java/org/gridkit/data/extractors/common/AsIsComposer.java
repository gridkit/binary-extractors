package org.gridkit.data.extractors.common;

import java.util.ArrayList;
import java.util.List;

class AsIsComposer implements CompositeExtractor.ValueComposer {

	private List<Object> result = new ArrayList<Object>();
	
	@Override
	public void push(int id, Object part) {
		if (id == 0) {
			result.add(part);
		}
		else {
			throw new IllegalArgumentException("No such parameter: " + id);
		}
	}

	@Override
	public void compose(ResultVectorReceiver output, int outputIndex) {
		for(Object v: result) {
			output.push(outputIndex, v);
		}
		
	}
}
