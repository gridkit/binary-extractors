package org.gridkit.data.extractors.common;

public class BooleanBinaryPredicate extends AbstractBinaryFunction<Boolean, Boolean, Boolean> {

	private static final long serialVersionUID = 20130906L;

	public static enum Op {
		AND,
		OR,
		XOR
	}

	public static Op AND = Op.AND;
	public static Op OR = Op.OR;
	public static Op XOR = Op.XOR;
	
	private Op op;
	
	/**
	 * @deprecated left public for reflection based serialization
	 */
	public BooleanBinaryPredicate() {
		super();
	}

	public BooleanBinaryPredicate(Op op, BinaryExtractor<Boolean> left, BinaryExtractor<Boolean> right) {
		super(left, right);
		this.op = op;
	}

	@Override
	public Object getOperationToken() {
		return op;
	}

	@Override
	protected Boolean evaluate(Boolean left, Boolean right) {
		switch(op) {
		case AND: return left && right;
		case OR: return left || right;
		case XOR: return left ^ right;		
		}
		throw new IllegalArgumentException("Unknown binary operation " + op);
	}

	@Override
	protected String getDescription() {
		return op.toString();
	}
	
	
}
