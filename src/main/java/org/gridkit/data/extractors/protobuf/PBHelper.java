package org.gridkit.data.extractors.protobuf;

import java.nio.ByteBuffer;

import org.gridkit.data.extractors.common.BinaryReader;

class PBHelper {
	
//	public static CodedInputStream inputStream(ByteBuffer buffer) {
//		if (buffer.isDirect()) {
//			byte[] data = new byte[buffer.remaining()];
//			buffer.duplicate().get(data);
//			return CodedInputStream.newInstance(data);
//		}
//		else {
//			return CodedInputStream.newInstance(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
//		}
//	}

	public static ProtoBufCodedStream inputStream(ByteBuffer buffer) {
		BinaryReader reader = new BinaryReader.ByteBufferReader(buffer);
		return new ProtoBufCodedStream.BinaryReaderStream(reader);
	}
}
