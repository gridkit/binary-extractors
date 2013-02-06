package org.gridkit.data.extractors.common;

import java.nio.ByteBuffer;

public class Extractors {

	@SuppressWarnings("unchecked")
	public static <V> V extract(ByteBuffer binary, BinaryExtractor<V> extractor) {
		BinaryExtractorSet set = extractor.newExtractorSet();
		final int id = set.addExtractor(extractor);
		set.compile();
		final Object[] result = new Object[1];
		set.extractAll(binary, new ResultVectorReceiver() {
			@Override
			public void push(int pid, Object part) {
				if (id == pid) {
					result[0] = part;
				}
				else {
					throw new IllegalArgumentException("Unknown argiment ID: " + pid);
				}
				
			}
		});
		return (V) result[0];
	}	

	public static Object[] extractAll(ByteBuffer binary, BinaryExtractor<?>... extractors) {
		CompositeExtractorSet set = new CompositeExtractorSet();
		final int[] idmap = new int[extractors.length];
		for(int i = 0; i != idmap.length; ++i) {
			idmap[i] = set.addExtractor(extractors[i]);
		}
		
		final Object[] result = new Object[idmap.length];
		set.extractAll(binary, new ResultVectorReceiver() {
			@Override
			public void push(int pid, Object part) {
				for(int i = 0; i != idmap.length; ++i) {
					if (idmap[i] == pid) {
						result[i] = part;
					}
				}
			}
		});
		return result;
	}	
}
