package org.gridkit.data.extractors.common;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TreeEncoder {

	public List<Object> decode(ObjectDecoder decoder, CharSequence text) {
		List<Object> result = new ArrayList<Object>();
		CharSequenceStreamReader reader = new CharSequenceStreamReader(decoder, CharBuffer.wrap(text));
		while(!reader.isEOF()) {
			result.add(reader.readObject());
		}
		return result;
	}

	public void encode(ObjectEncoder encoder, Writer ws, Object object) {
		CharSequenceStreamWriter writer = new CharSequenceStreamWriter(encoder, ws);
		writer.writeObject(object);
	}	
	
	public static interface ObjectDecoder {
		
		public boolean isCompeteId(String idCandidate);
		
		public Object decode(String id, StreamReader reader);
		
	}
	
	public static interface ObjectEncoder {
	
		public String getClassId(Object obj);

		public void encode(StreamWriter writer, Object obj);
		
	}
	
	public static interface StreamReader {

		public boolean isEOF();
		
		public long readLong();
		
		public double readDouble();
		
		public long readTimestamp();
		
		public String readString();
		
		public byte[] readBytes();
		
		public Object readLiteral();
		
		public Object readObject();
		
		public StreamReader swicthDecoder(ObjectDecoder decoder);
		
	}

	public static interface StreamWriter {
		
		public void write(String v);

		public void write(int v);

		public void writeTyped(int v);
		
		public void writeTimestampe(long ts);
		
		public void write(long v);

		public void writeTyped(long v);

		public void write(float v);

		public void writeTyped(float v);

		public void write(double v);

		public void writeTyped(double v);

		public void write(byte[] blob);
		
		public void writeLiteral(Object lit);
		
		public void writeObject(Object obj);
		
		public StreamWriter swicthDecoder(ObjectEncoder encoder);
		
	}
	
	private static class CharSequenceStreamReader implements StreamReader {

		private ObjectDecoder decoder;
		private CharBuffer buffer;
		
		public CharSequenceStreamReader(ObjectDecoder decoder, CharBuffer buffer) {
			this.decoder = decoder;
			this.buffer = buffer;
		}
		
		@Override
		public boolean isEOF() {
			return buffer.remaining() == 0;
		}

		@Override
		public long readLong() {
			Object v = readLiteral();
			if (v instanceof Number) {
				return ((Number)v).longValue();
			}
			else {
				return Long.parseLong(v.toString());
			}
		}

		@Override
		public double readDouble() {
			Object v = readLiteral();
			if (v instanceof Number) {
				return ((Number)v).doubleValue();
			}
			else {
				return Double.parseDouble(v.toString());
			}
		}

		@Override
		public long readTimestamp() {
			String str = readString();			
			return decodeTimestamp(str);
		}

		@Override
		public String readString() {
			Object v = readLiteral();
			if (v instanceof byte[]) {
				return StringUtils.newStringUtf8((byte[])v);
			}
			else {
				return v.toString();
			}
		}

		@Override
		public byte[] readBytes() {
			return (byte[])readLiteral();
		}

		@Override
		public Object readLiteral() {
			char ch = readChar();
			if (ch == '~') {
				// quoted string
				String str = readStringUntil('~');
				return str;
			}
			else if (ch == '_') {
				char type = readChar();
				if (type == '.') {
					return null;
				}
				else {
					String base64 = readStringUntil('.');
					switch(type) {
						case 's': return decodeStringB64(base64);
						case 'b': return decodeBytesB64(base64);
						case 'i': return decodeIntB64(base64);
						case 'l': return decodeLongB64(base64);
						case 'f': return decodeFloatB64(base64);
						case 'd': return decodeDoubleB64(base64);
						default: throw new IllegalArgumentException("Type '" + type + "' is not defined");
					}
				}
			}
			else {
				String text = readStringUntil('.');
				return text;
			}
		}

		private char readChar() {
			return buffer.get();
		}

		private String readStringUntil(char ch) {
			int p = buffer.position();
			while(buffer.get() != ch);
			int l = buffer.position() - 1;
			char[] cb = new char[l - p];
			buffer.position(p);
			buffer.get(cb);
			buffer.get();
			return new String(cb);
		}

		@Override
		public Object readObject() {
			StringBuilder sb = new StringBuilder();
			sb.append(buffer.get());
			while(!decoder.isCompeteId(sb.toString())) {
				sb.append(buffer.get());
			}
			return decoder.decode(sb.toString(), this);
		}

		@Override
		public StreamReader swicthDecoder(ObjectDecoder decoder) {
			return new CharSequenceStreamReader(decoder, buffer);
		}
	}
	
	private static class CharSequenceStreamWriter implements StreamWriter {

		private ObjectEncoder encoder;
		private Writer writer;
		
		public CharSequenceStreamWriter(ObjectEncoder encoder, Writer writer) {
			this.encoder = encoder;
			this.writer = writer;
		}
		
		@Override
		public void write(String v) {
			try {
				if (isSafeString(v)) {
					if (isSuperSafeString(v)) {
						writer.append(v);
						writer.append('.');
					}
					else {
						writer.append('~');
						writer.append(v);
						writer.append('~');
					}
				}
				else {
					writer.append("_s");
					writer.append(encodeStringB64(v));
					writer.append('.');				
				}
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public void write(int v) {
			write(String.valueOf(v));
		}

		@Override
		public void writeTyped(int v) {
			writeBase64('i', encodeIntB64(v));
		}

		@Override
		public void write(long v) {
			write(String.valueOf(v));
		}

		@Override
		public void writeTyped(long v) {
			writeBase64('l', encodeLongB64(v));
		}
		
		@Override
		public void write(float v) {
			write(String.valueOf(v));
		}

		@Override
		public void writeTyped(float v) {
			writeBase64('f', encodeFloatB64(v));
		}
		
		
		@Override
		public void write(double v) {
			write(String.valueOf(v));
		}

		@Override
		public void writeTyped(double v) {
			writeBase64('d', encodeDoubleB64(v));
		}
		
		@Override
		public void write(byte[] blob) {
			writeBase64('b', encodeBytesB64(blob));
		}
		
		private void writeBase64(char type, String base64) {
			try {
				writer.append('_');
				writer.append(type);
				writer.append(base64);
				writer.append('.');
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public void writeTimestampe(long ts) {
			write(encodeTimestamp(ts));
		}

		@Override
		public void writeLiteral(Object lit) {
			try {
				if (lit == null) {
					// null literal
					writer.append("_.");
				}
				else if (lit instanceof String) {
					write((String)lit);
				}
				else if (lit instanceof Number) {
					write(String.valueOf(lit));
				}
				else if (lit instanceof byte[]) {
					write((byte[])lit);
				}
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public void writeObject(Object obj) {
			try {
				String type = encoder.getClassId(obj);
				if (!isUrlSafeString(type)) {
					throw new IllegalArgumentException("TypeId " + type + " is not URL safe");
				}
				writer.append(type);
				encoder.encode(this, obj);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public StreamWriter swicthDecoder(ObjectEncoder encoder) {
			return null;
		}
	}
	
	private static final boolean[] STRING_LITERAL_SAFE = new boolean['~' + 1];
	static {
		for(int c = '0'; c <= '9'; ++c) {
			STRING_LITERAL_SAFE[c] = true;
		}
		for(int c = 'A'; c <= 'Z'; ++c) {
			STRING_LITERAL_SAFE[c] = true;
		}
		for(int c = 'a'; c <= 'z'; ++c) {
			STRING_LITERAL_SAFE[c] = true;
		}
		STRING_LITERAL_SAFE['-'] = true;
		STRING_LITERAL_SAFE['_'] = true;
		STRING_LITERAL_SAFE['.'] = true;
		STRING_LITERAL_SAFE['~'] = false;
	}

	private static final boolean[] STRING_URL_SAFE = new boolean['~' + 1];
	static {
		for(int c = '0'; c <= '9'; ++c) {
			STRING_URL_SAFE[c] = true;
		}
		for(int c = 'A'; c <= 'Z'; ++c) {
			STRING_URL_SAFE[c] = true;
		}
		for(int c = 'a'; c <= 'z'; ++c) {
			STRING_URL_SAFE[c] = true;
		}
		STRING_URL_SAFE['-'] = true;
		STRING_URL_SAFE['_'] = true;
		STRING_URL_SAFE['.'] = true;
		STRING_URL_SAFE['~'] = true;
	}
	
	static String encodeTimestamp(long ts) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
		return sdf.format(new Date(ts));
	}

	static long decodeTimestamp(String str) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
			return sdf.parse(str).getTime();
		} catch (ParseException e) {
			throw new IllegalArgumentException("'" + str + "' cannot be parsed as date");
		}
	}
	
	static String encodeStringB64(String text) {
		byte[] binary = StringUtils.getBytesUtf8(text);
		return encodeUrlSafeBase64(binary);
	}

	private static String encodeUrlSafeBase64(byte[] binary) {
		return StringUtils.newStringUtf8(Base64.encodeBase64(binary, false, true));
	}

	static String decodeStringB64(String text) {
		byte[] utf8 = Base64.decodeBase64(text);
		return StringUtils.newStringUtf8(utf8);
	}

	static String encodeIntB64(int v) {
		byte[] dv = new byte[4];
		ByteBuffer.wrap(dv).putInt(v);
		return encodeUrlSafeBase64(dv);		
	}
	
	static int decodeIntB64(String text) {
		return ByteBuffer.wrap(Base64.decodeBase64(text)).getInt();		
	}

	static String encodeLongB64(long v) {
		byte[] dv = new byte[8];
		ByteBuffer.wrap(dv).putLong(v);
		return encodeUrlSafeBase64(dv);		
	}
	
	static long decodeLongB64(String text) {
		return ByteBuffer.wrap(Base64.decodeBase64(text)).getLong();		
	}

	static String encodeFloatB64(float v) {
		byte[] dv = new byte[4];
		ByteBuffer.wrap(dv).putFloat(v);
		return encodeUrlSafeBase64(dv);		
	}

	static float decodeFloatB64(String text) {
		return ByteBuffer.wrap(Base64.decodeBase64(text)).getFloat();		
	}

	static String encodeDoubleB64(double v) {
		byte[] dv = new byte[8];
		ByteBuffer.wrap(dv).putDouble(v);
		return encodeUrlSafeBase64(dv);
	}

	static double decodeDoubleB64(String text) {
		return ByteBuffer.wrap(Base64.decodeBase64(text)).getDouble();
	}

	static String encodeBytesB64(byte[] bytes) {
		return encodeUrlSafeBase64(bytes);		
	}
	
	static byte[] decodeBytesB64(String text) {
		return Base64.decodeBase64(text);
	}
	
	/**
	 * "Safe" string should only contain characters from RFC 3986 "Unreserved Characters" (section 2.3)
	 * <br/>
	 * In addition, "~" is forbidden too.
	 *  
	 * @param text
	 */
	static boolean isSafeString(String text) {
		if (text.length() == 0) {
			return false;
		}
		for(int i = 0; i != text.length(); ++i) {
			int c = text.charAt(i);
			if (c >= STRING_LITERAL_SAFE.length || !STRING_LITERAL_SAFE[c]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * "Safe" string should only contain characters from RFC 3986 "Unreserved Characters" (section 2.3)
	 *  
	 * @param text
	 */
	static boolean isUrlSafeString(String text) {
		if (text.length() == 0) {
			return false;
		}
		for(int i = 0; i != text.length(); ++i) {
			int c = text.charAt(i);
			if (c >= STRING_URL_SAFE.length || !STRING_URL_SAFE[c]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Verifies that
	 * - string is not empty
	 * - first character is letter
	 * - there is no '.' if the string
	 * 
	 * @param text
	 * @return
	 */
	static boolean isSuperSafeString(String text) {
		if (text.isEmpty()) {
			return false;
		}
		else if (text.charAt(0) != '_') {
			return false;
		}
		else if (!isDotSafe(text)) {
			return false;
		}
		return true;
	}
	
	static boolean isDotSafe(String text) {
		for(int i = 0; i != text.length(); ++i) {
			int c = text.charAt(i);
			if (c == '.') {
				return false;
			}
		}
		return true;
	}
}
