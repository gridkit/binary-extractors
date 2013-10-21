package org.gridkit.data.extractors.common;

import java.nio.ByteBuffer;

public class BlobWrapperExtractor extends AbstractValueTransformer<Object, Blob> {

	private static final long serialVersionUID = 20130905L;

	/**
	 * @deprecated left public for external serialization
	 */
	public BlobWrapperExtractor() {
		super();
	}

	@SuppressWarnings("unchecked")
	public BlobWrapperExtractor(BinaryExtractor<?> sourceExtractor) {
		super((BinaryExtractor<Object>)sourceExtractor);
	}

	@Override
	public Object getOperationToken() {
		return getClass();
	}

	@Override
	protected Blob transform(Object input) {
		if (input instanceof Blob) {
			return (Blob)input;
		}
		else if (input instanceof byte[]) {
			return new Blob((byte[])input);
		}
		else if (input instanceof ByteBuffer) {
			return new Blob((ByteBuffer)input);
		}
		throw new IllegalArgumentException("Cannot blobbify " + input);
	}

	@Override
	protected String getDescription() {
		return "asBlob";
	}
}
