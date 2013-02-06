package org.gridkit.data.extractors.protobuf;

import java.util.Arrays;

import org.gridkit.data.extractors.common.ResultVectorReceiver;

/**
 * Simple array backed class to map low scale integers.
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
class Int2Int {
	
	private int[] map;
	
	public void set(int index, int value) {
		if (map == null) {
			map = new int[index + 1];
			Arrays.fill(map, -1);
		}
		else if (map.length <= index) {
			int n = map.length;
			map = Arrays.copyOf(map, index + 1);
			Arrays.fill(map, n, map.length, -1);
		}
		map[index] = value;
	}
	
	public int get(int index) {
		return map == null || index >= map.length ? -1 : map[index];
	}
	
	public ResultVectorReceiver newMapper(final ResultVectorReceiver receiver) {
		return new ResultVectorReceiver() {
			@Override
			public void push(int id, Object part) {
				int nid = get(id);
				if (nid < 0) {
					throw new IllegalArgumentException("Index " + id + " is not mapped");
				}
				else {
					receiver.push(nid, part);
				}
			}
		};
	}
	
	public String toString() {
		return Arrays.toString(map);
	}
	
}
