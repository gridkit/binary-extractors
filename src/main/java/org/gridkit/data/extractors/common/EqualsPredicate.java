package org.gridkit.data.extractors.common;

import java.util.Arrays;
import java.util.List;

public class EqualsPredicate extends AbstractCompositeExtractor<Boolean> {

	private static final long serialVersionUID = 20130205L;
	
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
	public ValueComposer newComposer() {
		return new EqualsComposer();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((left == null) ? 0 : left.hashCode());
		result = prime * result + ((right == null) ? 0 : right.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EqualsPredicate other = (EqualsPredicate) obj;
		if (left == null) {
			if (other.left != null)
				return false;
		} else if (!left.equals(other.left))
			return false;
		if (right == null) {
			if (other.right != null)
				return false;
		} else if (!right.equals(other.right))
			return false;
		return true;
	}

	public String toString() {
		return "eq(" + left + ", " + right + ")"; 
	}
	
	private static class EqualsComposer implements ValueComposer {
		
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
		public void compose(ScalarResultReceiver output) {
			if (!leftDefined || !rightDefined) {
				return;
			}
			else {
				if (left == right) {
					output.push(Boolean.TRUE);
				}
				else if (left == null || right == null) {
					output.push(Boolean.FALSE);
				}
				else {
					output.push(left.equals(right));
				}
			}
		}
	}
}
