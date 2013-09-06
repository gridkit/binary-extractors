package org.gridkit.data.extractors.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract class for implementing binary functions
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public abstract class AbstractBinaryFunction<Left, Right, Result> extends AbstractCompositeExtractor<Result> implements Serializable {

	private static final long serialVersionUID = 20130906L;
	
	private BinaryExtractor<Left> left;
	private BinaryExtractor<Right> right;
	
	// may be needed for reflection based deserialization
	public AbstractBinaryFunction() {
		this(null, null);
	}

	public AbstractBinaryFunction(BinaryExtractor<Left> left, BinaryExtractor<Right> right) {
		this.left = left;
		this.right = right;
	}

	protected abstract Result evaluate(Left left, Right right); 
	
	@Override
	@SuppressWarnings("unchecked")
	public List<BinaryExtractor<?>> getSubExtractors() {
		return Arrays.asList(left, right);
	}

	@Override
	public ValueComposer newComposer() {
		return new ValueComposer() {
			
			private Object left;
			private boolean hasLeft;
			private Object right;
			private boolean hasRight;
			
			@Override
			public void push(int id, Object part) {
				if (id == 0) {
					if (hasLeft) {
						throw new IllegalArgumentException("Single value is expected");
					}
					left = part;
					hasLeft = true;
				}
				else if (id == 1) {
					if (hasRight) {
						throw new IllegalArgumentException("Single value is expected");
					}
					right = part;
					hasRight = true;
				}
				else {
					throw new IndexOutOfBoundsException("Param index " + id + " is out of bounds");
				}
			}
			
			@Override
			@SuppressWarnings("unchecked")
			public void compose(ScalarResultReceiver receiver) {
				if (hasLeft && hasRight) {
					Object r = evaluate((Left)left, (Right)right);
					receiver.push(r);
				}
			}
		};
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
	@SuppressWarnings("rawtypes")
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AbstractBinaryFunction other = (AbstractBinaryFunction) obj;
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

	protected abstract String getDescription();
	
	@Override
	public String toString() {
		return  getDescription() + "(" + left + ", " + right + ")";
	}
}
