package org.gridkit.data.extractors.common;


class LastComposer implements CompositeExtractor.ValueComposer {

	private boolean hasResult;
	private Object result;
	
	@Override
	public void push(int id, Object part) {
		if (id == 0) {
			hasResult = true;
			result = part;
		}
		else {
			throw new IllegalArgumentException("No such parameter: " + id);
		}
	}

	@Override
	public void compose(ResultVectorReceiver output, int outputIndex) {
		if (hasResult) {
			output.push(outputIndex, result);
		}
		
	}
}
