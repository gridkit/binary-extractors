package org.gridkit.data.extractors.common;

import java.util.Arrays;
import java.util.List;

public class EqualsPredicate extends AbstractCompositeExtractor<Boolean> {

	private final BinaryExtractor<?> left;
	private final BinaryExtractor<?> right;
	
	public EqualsPredicate(BinaryExtractor<?> left, BinaryExtractor<?> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<BinaryExtractor<?>> getSubExtractors() {
		return Arrays.asList(left, right);
	}

	@Override
	public org.gridkit.data.extractors.common.CompositeExtractor.ValueComposer<Boolean> newComposer() {
		return new EqualsComposer();
	}
	
	public String toString() {
		return "eq[" + left + ", " + right + "]"; 
	}
	
	private static class EqualsComposer implements ValueComposer<Boolean> {
		
		private Object left;
		private boolean leftDefined;		
		private Object right;
		private boolean rightDefined;
		
		@Override
		public void push(int id, Object part) {
			if (id == 0) {
				left = part;
				leftDefined = true;
			}
			else if (id == 1) {
				right = part;
				rightDefined = true;
			}
			else {
				throw new IllegalArgumentException("No such parameter: " + id);
			}
		}

		@Override
		public void compose(ResultVectorReceiver output, int outputIndex) {
			if (!leftDefined || !rightDefined) {
				return;
			}
			else {
				if (left == right) {
					output.push(outputIndex, Boolean.TRUE);
				}
				else if (left == null || right == null) {
					output.push(outputIndex, Boolean.FALSE);
				}
				else {
					output.push(outputIndex, left.equals(right));
				}
			}
		}
	}
}
