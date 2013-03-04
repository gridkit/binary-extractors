package org.gridkit.data.extractors.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ComparisonPredicate extends AbstractCompositeExtractor<Boolean> implements Serializable {

	private static final long serialVersionUID = 20130227L;
	
	public enum Op {
		EQ, GT, LT, GE, LE
	}
	
	private Op predicateOp;
	private BinaryExtractor<?> left;
	private BinaryExtractor<?> right;
	private Comparator<?> comparator;
	
	public ComparisonPredicate(Op predicateOp, BinaryExtractor<?> left, BinaryExtractor<?> right) {
		this(predicateOp, left, right, null);
	}

	public ComparisonPredicate(Op predicateOp, BinaryExtractor<?> left, BinaryExtractor<?> right, Comparator<?> comparator) {
		this.predicateOp = predicateOp;
		this.left = left;
		this.right = right;
		this.comparator = comparator;
	}

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
					left = part;
					hasLeft = true;
				}
				else if (id == 1) {
					right = part;
					hasRight = true;
				}
				else {
					throw new IndexOutOfBoundsException("Param index " + id + " is out of bounds");
				}
			}
			
			@Override
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public void compose(ScalarResultReceiver receiver) {
				if (hasLeft && hasRight) {
					if (comparator == null) {
						if (left instanceof Comparable && right instanceof Comparable) {
							try {
								int n = ((Comparable)left).compareTo(right);
								interpret(receiver, n);
							} catch (Exception e) {
								// ignore errors
							}
						}
					}
					else {
						try {
							int n = ((Comparator)comparator).compare(left, right);
							interpret(receiver, n);
						} catch (Exception e) {
							// ignore errors
						}
					}
				}
			}

			protected void interpret(ScalarResultReceiver receiver, int n) {
				switch(predicateOp) {
				case EQ: receiver.push(n == 0); break;
				case GT: receiver.push(n > 0); break;
				case GE: receiver.push(n >= 0); break;
				case LT: receiver.push(n < 0); break;
				case LE: receiver.push(n <= 0); break;
				default:
				}
			}
		};
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((comparator == null) ? 0 : comparator.hashCode());
		result = prime * result + ((left == null) ? 0 : left.hashCode());
		result = prime * result
				+ ((predicateOp == null) ? 0 : predicateOp.hashCode());
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
		ComparisonPredicate other = (ComparisonPredicate) obj;
		if (comparator == null) {
			if (other.comparator != null)
				return false;
		} else if (!comparator.equals(other.comparator))
			return false;
		if (left == null) {
			if (other.left != null)
				return false;
		} else if (!left.equals(other.left))
			return false;
		if (predicateOp != other.predicateOp)
			return false;
		if (right == null) {
			if (other.right != null)
				return false;
		} else if (!right.equals(other.right))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return predicateOp + (comparator == null ? "" : "/" + comparator.toString()) + "(" + left + ", " + right + ")";
	}
}
