package org.gridkit.data.extractors.common;


class FirstComposer implements CompositeExtractor.ValueComposer {

	private boolean hasResult;
	private Object result;
	
	@Override
	public void push(int id, Object part) {
		if (id == 0) {
			if (!hasResult) {
				hasResult = true;
				result = part;
			}
		}
		else {
			throw new IllegalArgumentException("No such parameter: " + id);
		}
	}

	@Override
	public void compose(ScalarResultReceiver output) {
		if (hasResult) {
			output.push(result);
		}
	}
}
