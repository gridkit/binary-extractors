package org.gridkit.data.extractors.common;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * This is simple wrapper around byte array implementing, {@link #equals(Object)} and {@link Blob#co
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public final class Blob implements Comparable<Blob>, Serializable {

	private static final long serialVersionUID = 20130123L;
	
	private final byte[] data;
	private final int hash;
	
	public Blob(ByteBuffer buf) {
		data = new byte[buf.remaining()];
		buf.get(data);
		hash = calcHash();
	}
	
	public Blob(byte[] data) {
		this(ByteBuffer.wrap(data));
	}
	
	private int calcHash() {
		CRC32 crc = new CRC32();
		crc.update(data);
		return (int)crc.getValue();
	}

	public byte[] toArrays() {
		return Arrays.copyOf(data, data.length);
	}

	public ByteBuffer toBuffer() {
		return ByteBuffer.wrap(data).asReadOnlyBuffer();
	}
	
	@Override
	public int hashCode() {
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		else if (obj == this) {
			return true;
		}
		else if (obj instanceof Blob) {
			Blob that = (Blob) obj;
			if (hash != that.hash) {
				return false;
			}
			return Arrays.equals(data, that.data);
		}
		else {
			return false;
		}
	}

	@Override
	public int compareTo(Blob o) {
		if (this == o) {
			return 0;
		}
		byte[] a = data;
		byte[] b = o.data;
		int n = 0;
		while(true) {
			if (n >= a.length && n >= b.length) {
				return 0;
			}
			else if (n >= a.length) {
				return -256;
			}
			else if (n >= b.length) {
				return 256;
			}
			else {
				int c = (0xFF & (int)a[n]) - (0xFF & (int)b[n]);
				if (c != 0) {
					return c;
				}
				else {
					++n;
				}
			}
		}
	}
}
