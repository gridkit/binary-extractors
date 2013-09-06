package org.gridkit.data.extractors.common;

public class NotTransformer extends AbstractValueTransformer<Boolean, Boolean> {

	private static final long serialVersionUID = 20130905L;

	/**
	 * @deprecated left public for reflection based deserialization
	 */
	public NotTransformer() {
		super();
	}

	public NotTransformer(BinaryExtractor<Boolean> sourceExtractor) {
		super(sourceExtractor);
	}

	@Override
	protected Boolean transform(Boolean input) {
		return Boolean.valueOf(!input.booleanValue());
	}

	@Override
	protected String getDescription() {
		return "NOT";
	}
}
