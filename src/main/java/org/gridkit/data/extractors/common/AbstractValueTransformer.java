package org.gridkit.data.extractors.common;

import java.util.Collections;
import java.util.List;

/**
 * Common base class for various scalar object parsers.
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 *
 * @param <InType>
 * @param <OutType>
 */
public abstract class AbstractValueTransformer<InType, OutType> extends AbstractCompositeExtractor<OutType> {
	
	private static final long serialVersionUID = 20130609L;

	/** Cloud be null */
	protected BinaryExtractor<InType> sourceExtractor;

	public AbstractValueTransformer() {
		this(null);
	}
	
	public AbstractValueTransformer(BinaryExtractor<InType> sourceExtractor) {
		this.sourceExtractor = sourceExtractor;
	}
	
	@Override
	public List<BinaryExtractor<?>> getSubExtractors() {
		if (sourceExtractor == null) {
			return Collections.<BinaryExtractor<?>>singletonList(VerbatimExtractor.INSTANCE);
		}
		else {
			return Collections.<BinaryExtractor<?>>singletonList(sourceExtractor);
		}
	}

	protected abstract OutType transform(InType input);

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((sourceExtractor == null) ? 0 : sourceExtractor.hashCode());
		return result;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AbstractValueTransformer other = (AbstractValueTransformer) obj;
		if (sourceExtractor == null) {
			if (other.sourceExtractor != null)
				return false;
		} else if (!sourceExtractor.equals(other.sourceExtractor))
			return false;
		return true;
	}

	public String toString() {
		return sourceExtractor == null ? "" : sourceExtractor + "/"; 
	}
	
	@Override
	public org.gridkit.data.extractors.common.CompositeExtractor.ValueComposer newComposer() {
		return new CompositeExtractor.SingleArgumentComposer() {
			@Override
			@SuppressWarnings("unchecked")
			protected void processInput(Object input, ScalarResultReceiver receiver) {
				receiver.push(transform((InType)input));				
			}
		};
	}
}
