package org.gridkit.data.extractors.protobuf;

import java.io.IOException;

public class ProtocolBufferWireFormatException extends IOException {

	private static final long serialVersionUID = 20130610L;

	protected ProtocolBufferWireFormatException() {
		super();
	}

	protected ProtocolBufferWireFormatException(String message, Throwable cause) {
		super(message, cause);
	}

	protected ProtocolBufferWireFormatException(String message) {
		super(message);
	}

	protected ProtocolBufferWireFormatException(Throwable cause) {
		super(cause);
	}
	
	public static ProtocolBufferWireFormatException invalidTag(int tag) {
		return new ProtocolBufferWireFormatException("Invalid tag in stream (" + tag + "). Zero field is reserved.");
	}

	public static ProtocolBufferWireFormatException invalidWireType(int type) {
		return new ProtocolBufferWireFormatException("Unsupported wire type " + type + "");
	}

	public static ProtocolBufferWireFormatException groupTagUnsupported() {
		return new ProtocolBufferWireFormatException("Repeated group tag is not supported by parser");
	}

	public static ProtocolBufferWireFormatException malformedVarint() {
		return new ProtocolBufferWireFormatException("Malformed VARINT");
	}

	public static ProtocolBufferWireFormatException truncatedMessage() {
		return new ProtocolBufferWireFormatException("Truncated message");
	}

	public static ProtocolBufferWireFormatException negativeSize() {
		return new ProtocolBufferWireFormatException("Negative size for lenght delimited");
	}

	public static ProtocolBufferWireFormatException sizeLimitExceeded() {
		return new ProtocolBufferWireFormatException("Size limit exceeded");
	}
}
