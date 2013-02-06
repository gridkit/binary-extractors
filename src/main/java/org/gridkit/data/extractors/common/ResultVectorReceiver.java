package org.gridkit.data.extractors.common;

public interface ResultVectorReceiver {

	public void push(int id, Object part);
	
	public static class ResultVector implements ResultVectorReceiver {
		
		private final Object[] vector;
		
		public ResultVector(int size) {
			this.vector = new Object[size];
		}

		@Override
		public void push(int id, Object part) {
			vector[id] = part;
		}
		
		public Object[] getVector() {
			return vector;
		}
	}
}
