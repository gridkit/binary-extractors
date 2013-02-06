package org.gridkit.data.extractors.protobuf;

import java.nio.ByteBuffer;

import com.google.protobuf.CodedInputStream;

class PBHelper {
	
	public static CodedInputStream inputStream(ByteBuffer buffer) {
		if (buffer.isDirect()) {
			byte[] data = new byte[buffer.remaining()];
			buffer.duplicate().get(data);
			return CodedInputStream.newInstance(data);
		}
		else {
			return CodedInputStream.newInstance(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
		}
	}

}
