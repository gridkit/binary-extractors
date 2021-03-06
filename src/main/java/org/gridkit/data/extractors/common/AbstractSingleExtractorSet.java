package org.gridkit.data.extractors.common;


public abstract class AbstractSingleExtractorSet implements BinaryExtractorSet {

	private BinaryExtractor<?> instance;
	private boolean loaded;
	
	public AbstractSingleExtractorSet(BinaryExtractor<?> instance) {
		this.instance = instance;
	}
	
	public BinaryExtractor<?> getInstance() {
		return instance;
	}
	
	@Override
	public int addExtractor(BinaryExtractor<?> extractor) {
		if (instance == extractor) {
			loaded = true;
			return 0;
		}
		else {
			throw new IllegalArgumentException("Unsupported extractor");
		}
	}

	@Override
	public int getSize() {
		return loaded ? 1 : 0;
	}

	@Override
	public void compile() {
	}

	@Override
	public void extractAll(Object source, VectorResultReceiver resultReceiver) {
		resultReceiver.push(0, extract(source));		
	}
	
	protected abstract Object extract(Object source);
}
