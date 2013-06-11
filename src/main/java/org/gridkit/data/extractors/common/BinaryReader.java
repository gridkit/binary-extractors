package org.gridkit.data.extractors.common;

import java.lang.ref.PhantomReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public interface BinaryReader {

	public BinaryReader slice(int offs, int length);
	
	public int length();
	
	/**
	 * {@link ByteBuffer} may use {@link PhantomReference}, thus increase operation cost. 
	 * @return
	 */
	public ByteBuffer asBuffer();

	public byte getByte(int off);

	public char getChar(int off);
	
	public short getInt16(int off); 

	public int getInt32(int off); 

	public long getInt64(int off); 

	public float getFloat32(int off); 

	public double getFloat64(int off); 

	public void getBytes(byte[] target, int off, int arrayOff, int len);
	
	public short getInt16(int off, ByteOrder order); 
	
	public int getInt32(int off, ByteOrder order); 
	
	public long getInt64(int off, ByteOrder order); 
	
	public static class ByteBufferReader implements BinaryReader {
		
		private static volatile int BARRIER = 0;
		
		private final ByteBuffer buffer;
		private ByteBuffer littleEndian;
		
		public ByteBufferReader(ByteBuffer buffer) {
			this.buffer = buffer.order() == ByteOrder.BIG_ENDIAN ? buffer : buffer.slice().order(ByteOrder.BIG_ENDIAN);
		}

		@Override
		public BinaryReader slice(int offs, int lenght) {
			ByteBuffer buffer = this.buffer.slice();
			buffer.position(offs);
			buffer.limit(offs + lenght);
			return new ByteBufferReader(buffer);
		}

		@Override
		public int length() {
			return buffer.remaining();
		}

		@Override
		public ByteBuffer asBuffer() {
			return buffer.slice();
		}

		@Override
		public byte getByte(int off) {
			return buffer.get(off);
		}

		@Override
		public char getChar(int off) {
			return buffer.getChar(off);
		}

		@Override
		public short getInt16(int off) {
			return buffer.getShort(off);
		}

		@Override
		public int getInt32(int off) {
			return buffer.getInt(off);
		}

		@Override
		public long getInt64(int off) {
			return buffer.getLong(off);
		}

		@Override
		public float getFloat32(int off) {
			return buffer.getFloat(off);
		}

		@Override
		public double getFloat64(int off) {
			return buffer.getDouble(off);
		}

		@Override
		public void getBytes(byte[] target, int off, int arrayOff, int len) {
			ByteBuffer bb = buffer.slice();
			bb.position(off);
			bb.get(target, arrayOff, len);
		}

		@Override
		public short getInt16(int off, ByteOrder order) {
			if (order == ByteOrder.BIG_ENDIAN) {
				return getInt16(off);
			}
			else {
				ensureLittleEndian();
				return littleEndian.getShort(off);
			}
		}

		private void ensureLittleEndian() {
			if (littleEndian == null) {
				ByteBuffer bb = buffer.slice();
				bb.order(ByteOrder.LITTLE_ENDIAN);
				// I really wonder if that kind of memory barrier will ever work
				bb.position(BARRIER);
				littleEndian = bb;
			}			
		}

		@Override
		public int getInt32(int off, ByteOrder order) {
			if (order == ByteOrder.BIG_ENDIAN) {
				return getInt32(off);
			}
			else {
				ensureLittleEndian();
				return littleEndian.getInt(off);
			}
		}

		@Override
		public long getInt64(int off, ByteOrder order) {
			if (order == ByteOrder.BIG_ENDIAN) {
				return getInt64(off);
			}
			else {
				ensureLittleEndian();
				return littleEndian.getLong(off);
			}
		}
	}
}
