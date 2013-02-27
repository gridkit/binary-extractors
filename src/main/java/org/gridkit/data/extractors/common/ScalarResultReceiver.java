package org.gridkit.data.extractors.common;

public interface ScalarResultReceiver {

	public void push(Object part);
	
	public static class ScalarResult implements ScalarResultReceiver {
		
		private Object result;
		
		public ScalarResult() {
		}

		@Override
		public void push(Object part) {
			result = part;
		}
		
		public Object getScalar() {
			return result;
		}
	}	

	public static class VectorEntry implements ScalarResultReceiver {
		
		private final VectorResultReceiver receiver;
		private final int index; 
		
		public VectorEntry(VectorResultReceiver receiver, int index) {
			this.receiver = receiver;
			this.index = index;
		}

		@Override
		public void push(Object part) {
			receiver.push(index, part);
		}

		@Override
		public String toString() {
			return receiver.toString() + "[" + index + "]";
		}
	}	
}
