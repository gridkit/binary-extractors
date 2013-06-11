package org.gridkit.data.extractors.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.charset.Charset;

import org.gridkit.data.extractors.common.BinaryReader;

abstract class ProtoBufCodedStream {

	public static final byte[] EMPTY_BYTES = {};
	
	public static final int WIRETYPE_VARINT = 0;
	public static final int WIRETYPE_FIXED64 = 1;
	public static final int WIRETYPE_LENGTH_DELIMITED = 2;
	public static final int WIRETYPE_START_GROUP = 3;
	public static final int WIRETYPE_END_GROUP = 4;
	public static final int WIRETYPE_FIXED32 = 5;

	static final int TAG_TYPE_BITS = 3;
	static final int TAG_TYPE_MASK = (1 << TAG_TYPE_BITS) - 1;

	/** Given a tag value, determines the wire type (the lower 3 bits). */
	static int getTagWireType(final int tag) {
		return tag & TAG_TYPE_MASK;
	}

	/** Given a tag value, determines the field number (the upper 29 bits). */
	public static int getTagFieldNumber(final int tag) {
		return tag >>> TAG_TYPE_BITS;
	}

	/** Makes a tag value given a field number and wire type. */
	static int makeTag(final int fieldNumber, final int wireType) {
		return (fieldNumber << TAG_TYPE_BITS) | wireType;
	}

	// -----------------------------------------------------------------

	/**
	 * Attempt to read a field tag, returning zero if we have reached EOF.
	 * Protocol message parsers use this to read tags, since a protocol message
	 * may legally end wherever a tag occurs, and zero is not a valid tag
	 * number.
	 */
	public int readTag() throws IOException {
		if (isAtEnd()) {
			return 0;
		}

		int tag = readRawVarint32();
		if (getTagFieldNumber(tag) == 0) {
			throw ProtocolBufferWireFormatException.invalidTag(tag);
		}
		return tag;
	}

	/**
	 * Reads and discards a single field, given its tag value.
	 * 
	 * @return {@code false} if the tag is an endgroup tag, in which case
	 *         nothing is skipped. Otherwise, returns {@code true}.
	 */
	public boolean skipField(final int tag) throws IOException {
		switch (getTagWireType(tag)) {
		case WIRETYPE_VARINT:
			readInt32();
			return true;
		case WIRETYPE_FIXED64:
			readRawLittleEndian64();
			return true;
		case WIRETYPE_LENGTH_DELIMITED:
			skipRawBytes(readRawVarint32());
			return true;
		case WIRETYPE_START_GROUP:
		case WIRETYPE_END_GROUP:
			throw ProtocolBufferWireFormatException.groupTagUnsupported();
		case WIRETYPE_FIXED32:
			readRawLittleEndian32();
			return true;
		default:
			throw ProtocolBufferWireFormatException.invalidWireType(getTagWireType(tag));
		}
	}

	/**
	 * Reads and discards an entire message. This will read either until EOF or
	 * until an endgroup tag, whichever comes first.
	 */
	public void skipMessage() throws IOException {
		while (true) {
			final int tag = readTag();
			if (tag == 0 || !skipField(tag)) {
				return;
			}
		}
	}

	// -----------------------------------------------------------------

	/** Read a {@code double} field value from the stream. */
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readRawLittleEndian64());
	}

	/** Read a {@code float} field value from the stream. */
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readRawLittleEndian32());
	}

	/** Read a {@code uint64} field value from the stream. */
	public long readUInt64() throws IOException {
		return readRawVarint64();
	}

	/** Read an {@code int64} field value from the stream. */
	public long readInt64() throws IOException {
		return readRawVarint64();
	}

	/** Read an {@code int32} field value from the stream. */
	public int readInt32() throws IOException {
		return readRawVarint32();
	}

	/** Read a {@code fixed64} field value from the stream. */
	public long readFixed64() throws IOException {
		return readRawLittleEndian64();
	}

	/** Read a {@code fixed32} field value from the stream. */
	public int readFixed32() throws IOException {
		return readRawLittleEndian32();
	}

	/** Read a {@code bool} field value from the stream. */
	public boolean readBool() throws IOException {
		return readRawVarint32() != 0;
	}

	/** Read a {@code string} field value from the stream. */
	public String readString() throws IOException {
		final int size = readRawVarint32();

		if (size > getBytesUntilLimit()) {
			throw new BufferUnderflowException();
		}
		
		final String result = copyAsString(bufPos, size);
		bufPos += size;
		return result;
	}
	
	protected abstract String copyAsString(int pos, int size);

	/** Read a {@code bytes} field value from the stream. */
	public byte[] readBytes() throws IOException {
		final int size = readRawVarint32();
		if (size > getBytesUntilLimit()) {
			throw new BufferUnderflowException();
		}
		if (size == 0) {
			return EMPTY_BYTES;
		} else {
			byte[] result = copyAsBytes(bufPos, size);
			bufPos += size;
			return result;
		}
	}
	
	public BinaryReader limitedReader() {
		return asReader(bufPos, limit - bufPos);
	}

	protected abstract BinaryReader asReader(int pos, int size);

	protected abstract byte[] copyAsBytes(int pos, int size);

	/** Read a {@code uint32} field value from the stream. */
	public int readUInt32() throws IOException {
		return readRawVarint32();
	}

	/**
	 * Read an enum field value from the stream. Caller is responsible for
	 * converting the numeric value to an actual enum.
	 */
	public int readEnum() throws IOException {
		return readRawVarint32();
	}

	/** Read an {@code sfixed32} field value from the stream. */
	public int readSFixed32() throws IOException {
		return readRawLittleEndian32();
	}

	/** Read an {@code sfixed64} field value from the stream. */
	public long readSFixed64() throws IOException {
		return readRawLittleEndian64();
	}

	/** Read an {@code sint32} field value from the stream. */
	public int readSInt32() throws IOException {
		return decodeZigZag32(readRawVarint32());
	}

	/** Read an {@code sint64} field value from the stream. */
	public long readSInt64() throws IOException {
		return decodeZigZag64(readRawVarint64());
	}

	// =================================================================

	/**
	 * Read a raw Varint from the stream. If larger than 32 bits, discard the
	 * upper bits.
	 */
	public int readRawVarint32() throws IOException {
		byte tmp = readRawByte();
		if (tmp >= 0) {
			return tmp;
		}
		int result = tmp & 0x7f;
		if ((tmp = readRawByte()) >= 0) {
			result |= tmp << 7;
		} else {
			result |= (tmp & 0x7f) << 7;
			if ((tmp = readRawByte()) >= 0) {
				result |= tmp << 14;
			} else {
				result |= (tmp & 0x7f) << 14;
				if ((tmp = readRawByte()) >= 0) {
					result |= tmp << 21;
				} else {
					result |= (tmp & 0x7f) << 21;
					result |= (tmp = readRawByte()) << 28;
					if (tmp < 0) {
						// Discard upper 32 bits.
						for (int i = 0; i < 5; i++) {
							if (readRawByte() >= 0) {
								return result;
							}
						}
						throw ProtocolBufferWireFormatException.malformedVarint();
					}
				}
			}
		}
		return result;
	}

	/**
	 * Reads a varint from the input one byte at a time, so that it does not
	 * read any bytes after the end of the varint. If you simply wrapped the
	 * stream in a CodedInputStream and used
	 * {@link #readRawVarint32(InputStream)} then you would probably end up
	 * reading past the end of the varint since CodedInputStream buffers its
	 * input.
	 */
	static int readRawVarint32(final InputStream input) throws IOException {
		final int firstByte = input.read();
		if (firstByte == -1) {
			throw ProtocolBufferWireFormatException.truncatedMessage();
		}
		return readRawVarint32(firstByte, input);
	}

	/**
	 * Like {@link #readRawVarint32(InputStream)}, but expects that the caller
	 * has already read one byte. This allows the caller to determine if EOF has
	 * been reached before attempting to read.
	 */
	public static int readRawVarint32(final int firstByte,
			final InputStream input) throws IOException {
		if ((firstByte & 0x80) == 0) {
			return firstByte;
		}

		int result = firstByte & 0x7f;
		int offset = 7;
		for (; offset < 32; offset += 7) {
			final int b = input.read();
			if (b == -1) {
				throw ProtocolBufferWireFormatException.truncatedMessage();
			}
			result |= (b & 0x7f) << offset;
			if ((b & 0x80) == 0) {
				return result;
			}
		}
		// Keep reading up to 64 bits.
		for (; offset < 64; offset += 7) {
			final int b = input.read();
			if (b == -1) {
				throw ProtocolBufferWireFormatException.truncatedMessage();
			}
			if ((b & 0x80) == 0) {
				return result;
			}
		}
		throw ProtocolBufferWireFormatException.malformedVarint();
	}

	/** Read a raw Varint from the stream. */
	public long readRawVarint64() throws IOException {
		int shift = 0;
		long result = 0;
		while (shift < 64) {
			final byte b = readRawByte();
			result |= (long) (b & 0x7F) << shift;
			if ((b & 0x80) == 0) {
				return result;
			}
			shift += 7;
		}
		throw ProtocolBufferWireFormatException.malformedVarint();
	}

	/** Read a 32-bit little-endian integer from the stream. */
	public int readRawLittleEndian32() throws IOException {
		final byte b1 = readRawByte();
		final byte b2 = readRawByte();
		final byte b3 = readRawByte();
		final byte b4 = readRawByte();
		return (((int) b1 & 0xff)) | (((int) b2 & 0xff) << 8)
				| (((int) b3 & 0xff) << 16) | (((int) b4 & 0xff) << 24);
	}

	/** Read a 64-bit little-endian integer from the stream. */
	public long readRawLittleEndian64() throws IOException {
		final byte b1 = readRawByte();
		final byte b2 = readRawByte();
		final byte b3 = readRawByte();
		final byte b4 = readRawByte();
		final byte b5 = readRawByte();
		final byte b6 = readRawByte();
		final byte b7 = readRawByte();
		final byte b8 = readRawByte();
		return (((long) b1 & 0xff)) | (((long) b2 & 0xff) << 8)
				| (((long) b3 & 0xff) << 16) | (((long) b4 & 0xff) << 24)
				| (((long) b5 & 0xff) << 32) | (((long) b6 & 0xff) << 40)
				| (((long) b7 & 0xff) << 48) | (((long) b8 & 0xff) << 56);
	}

	/**
	 * Decode a ZigZag-encoded 32-bit value. ZigZag encodes signed integers into
	 * values that can be efficiently encoded with varint. (Otherwise, negative
	 * values must be sign-extended to 64 bits to be varint encoded, thus always
	 * taking 10 bytes on the wire.)
	 * 
	 * @param n
	 *            An unsigned 32-bit integer, stored in a signed int because
	 *            Java has no explicit unsigned support.
	 * @return A signed 32-bit integer.
	 */
	public static int decodeZigZag32(final int n) {
		return (n >>> 1) ^ -(n & 1);
	}

	/**
	 * Decode a ZigZag-encoded 64-bit value. ZigZag encodes signed integers into
	 * values that can be efficiently encoded with varint. (Otherwise, negative
	 * values must be sign-extended to 64 bits to be varint encoded, thus always
	 * taking 10 bytes on the wire.)
	 * 
	 * @param n
	 *            An unsigned 64-bit integer, stored in a signed int because
	 *            Java has no explicit unsigned support.
	 * @return A signed 64-bit integer.
	 */
	public static long decodeZigZag64(final long n) {
		return (n >>> 1) ^ -(n & 1);
	}

	// -----------------------------------------------------------------

	protected int bufLimit;
	protected int bufferSizeAfterLimit;
	protected int bufPos;

//	private final byte[] buffer;
//	private final InputStream input;

	/** The absolute position of the end of the current message. */
	protected int limit = Integer.MAX_VALUE;

	/** See setSizeLimit() */
	private int sizeLimit = DEFAULT_SIZE_LIMIT;

	private static final int DEFAULT_SIZE_LIMIT = 64 << 20; // 64MB

	/**
	 * Set the maximum message size. In order to prevent malicious messages from
	 * exhausting memory or causing integer overflows, {@code CodedInputStream}
	 * limits how large a message may be. The default limit is 64MB. You should
	 * set this limit as small as you can without harming your app's
	 * functionality. Note that size limits only apply when reading from an
	 * {@code InputStream}, not when constructed around a raw byte array (nor
	 * with {@link ByteString#newCodedInput}).
	 * <p>
	 * If you want to read several messages from a single CodedInputStream, you
	 * could call {@link #resetSizeCounter()} after each one to avoid hitting
	 * the size limit.
	 * 
	 * @return the old limit.
	 */
	public int setSizeLimit(final int limit) {
		if (limit < 0) {
			throw new IllegalArgumentException(
					"Size limit cannot be negative: " + limit);
		}
		final int oldLimit = sizeLimit;
		sizeLimit = limit;
		return oldLimit;
	}

	/**
	 * Sets {@code currentLimit} to (current position) + {@code byteLimit}. This
	 * is called when descending into a length-delimited embedded message.
	 * 
	 * <p>
	 * Note that {@code pushLimit()} does NOT affect how many bytes the
	 * {@code CodedInputStream} reads from an underlying {@code InputStream}
	 * when refreshing its buffer. If you need to prevent reading past a certain
	 * point in the underlying {@code InputStream} (e.g. because you expect it
	 * to contain more data after the end of the message which you need to
	 * handle differently) then you must place a wrapper around you
	 * {@code InputStream} which limits the amount of data that can be read from
	 * it.
	 * 
	 * @return the old limit.
	 */
	public int pushLimit(int byteLimit) throws ProtocolBufferWireFormatException {
		if (byteLimit < 0) {
			throw ProtocolBufferWireFormatException.negativeSize();
		}
		byteLimit += bufPos;
		final int oldLimit = limit;
		if (byteLimit > oldLimit) {
			throw ProtocolBufferWireFormatException.truncatedMessage();
		}
		limit = byteLimit;

		return oldLimit;
	}

	/**
	 * Discards the current limit, returning to the previous limit.
	 * 
	 * @param oldLimit
	 *            The old limit, as returned by {@code pushLimit}.
	 */
	public void popLimit(final int oldLimit) {
		if (oldLimit > bufLimit) {
			throw new IndexOutOfBoundsException("Limit is beyond absolute data limit");
		}
		limit = oldLimit;
	}

	/**
	 * Returns the number of bytes to be read before the current limit. If no
	 * limit is set, returns -1.
	 */
	public int getBytesUntilLimit() {
		return limit - bufPos;
	}

	/**
	 * Returns true if the stream has reached the end of the input. This is the
	 * case if either the end of the underlying input source has been reached or
	 * if the stream has reached a limit created using {@link #pushLimit(int)}.
	 */
	public boolean isAtEnd() throws IOException {
		return bufPos == bufLimit || bufPos == limit;
	}

	/**
	 * The total bytes read up to the current position. Calling
	 * {@link #resetSizeCounter()} resets this value to zero.
	 */
	public int getTotalBytesRead() {
		return bufPos;
	}

	/**
	 * Read one byte from the input.
	 * 
	 * @throws InvalidProtocolBufferException
	 *             The end of the stream or the current limit was reached.
	 */
	public byte readRawByte() throws IOException {
		return byteAt(bufPos++);
	}

	protected abstract byte byteAt(int i);

	/**
	 * Reads and discards {@code size} bytes.
	 * 
	 * @throws InvalidProtocolBufferException
	 *             The end of the stream or the current limit was reached.
	 */
	public void skipRawBytes(final int size) throws IOException {
		if (size < 0) {
			throw ProtocolBufferWireFormatException.negativeSize();
		}

		if (bufPos + size > limit) {
			// Read to the end of the stream anyway.
			skipRawBytes(limit - bufPos);
			// Then fail.
			throw ProtocolBufferWireFormatException.truncatedMessage();
		}

		bufPos += size;
	}
	
	public static class BinaryReaderStream extends ProtoBufCodedStream {
		
		private static Charset UTF8 = Charset.forName("UTF8");
		
		
		private BinaryReader reader;

		public BinaryReaderStream(BinaryReader reader) {
			this.reader = reader;
			bufPos = 0;
			bufLimit = reader.length();
			limit = reader.length();
		}

		@Override
		protected BinaryReader asReader(int pos, int size) {
			return reader.slice(pos, size);
		}

		@Override
		protected String copyAsString(int pos, int size) {
			byte[] bytes = new byte[size];
			reader.getBytes(bytes, pos, 0, size);
			return new String(bytes, UTF8);
		}

		@Override
		protected byte[] copyAsBytes(int pos, int size) {
			byte[] bytes = new byte[size];
			reader.getBytes(bytes, pos, 0, size);
			return bytes;
		}

		@Override
		protected byte byteAt(int pos) {
			return reader.getByte(pos);
		}
	}
}
