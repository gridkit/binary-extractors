package org.gridkit.data.extractors;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.gridkit.data.extractors.common.BinaryExtractor;
import org.gridkit.data.extractors.common.Blob;
import org.gridkit.data.extractors.common.CompositeExtractorSet;
import org.gridkit.data.extractors.common.Extractors;
import org.gridkit.data.extractors.common.ResultVectorReceiver;
import org.junit.Assert;

abstract class BaseExtractionAssertTest implements ResultVectorReceiver {

	private Map<String, Integer> extractorMap = new HashMap<String, Integer>();
	private Map<Integer, Object> resultMap = new HashMap<Integer, Object>();
	private CompositeExtractorSet extractorSet = new CompositeExtractorSet();
	
	protected void addExtractor(String name, BinaryExtractor<?> extractor) {
		extractorMap.put(name, extractorSet.addExtractor(extractor));
	}
	
	protected void extract(byte[] data) {
		resultMap.clear();
		extractorSet.compile();
		extractorSet.extractAll(ByteBuffer.wrap(data), this);
	}
	
	protected void dump() {
		extractorSet.compile();
		System.out.println(Extractors.dump(extractorSet, 2));
	}
	
	@Override
	public void push(int id, Object part) {
		resultMap.put(id, part);
	}

	protected void assertValue(String name, Object value) {
		if (!extractorMap.containsKey(name)) {
			throw new IllegalArgumentException("Extractor '" + name + "' is not defined");
		}
		Assert.assertEquals("Extractor '" + name + "'", value, resultMap.get(extractorMap.get(name)));
	}
	
	protected byte[] getBytes(String rpath) {
		try {
			InputStream is = new FileInputStream("src/test/resources/" + rpath);
			Assert.assertNotNull("Resource '" + rpath + "' is missing", is);
			return toBytes(is);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected Blob blob(String text) {
		return new Blob(text.getBytes());
	}
	
	protected static byte[] toBytes(InputStream is) throws IOException {
		try {
			ByteArrayOutputStream buf = new ByteArrayOutputStream();			
			byte[] swap = new byte[1024];
			while(true) {
				int n = is.read(swap);
				if (n < 0) {
					break;
				}
				else {
					buf.write(swap, 0, n);
				}
			}
			return buf.toByteArray();
		}
		finally {
			try {
				is.close();
			}
			catch(Exception e) {
				// ignore
			}
		}
	}
}
