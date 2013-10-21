package org.gridkit.data.extractors.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gridkit.data.extractors.common.CompositeExtractor.ValueComposer;

public class CompositeExtractorSet implements BinaryExtractorSet, Serializable {

	private static final long serialVersionUID = 20130127L;
	
	private Map<Integer, List<Batch>> batches = new HashMap<Integer, List<Batch>>();
	private Map<CompositionToken, Integer> compositionIndex = new HashMap<CompositeExtractorSet.CompositionToken, Integer>();

	private List<ProcessingNode> nodes = new ArrayList<ProcessingNode>();
	private List<Integer> outs = new ArrayList<Integer>();
	
	// -1 is reserved for source object
	private int nBatchResult = -2;
	
	private Map<Integer, ValueLink> links = new HashMap<Integer, ValueLink>();
	
	private boolean compiled;

	@Override
	public int addExtractor(BinaryExtractor<?> extractor) {
		if (compiled) {
			throw new IllegalStateException("Cannot add extractor to a compiled set");
		}
		int id = addExtractor(-1, extractor);
		int r = outs.indexOf(id);
		if (r < 0) {
			r = outs.size();
			outs.add(id);
			addLink(id, new ResultVectorLink(r));
		}
		return r;
	}

	private int addExtractor(int sourceId, BinaryExtractor<?> extractor) {
		if (extractor == null || extractor instanceof VerbatimExtractor) {
			return sourceId; 
		}
		if (extractor instanceof ExtractorChain) {
			ExtractorChain chain = (ExtractorChain) extractor;
			BinaryExtractor<?> head = chain.getHead();
			int mid = addExtractor(sourceId, head);
			BinaryExtractor<?> tail = chain.getTail();
			return addExtractor(mid, tail);
		}
		if (extractor instanceof CompositeExtractor) {
			CompositeExtractor<?> ce = (CompositeExtractor<?>) extractor;
			List<BinaryExtractor<?>> el = ce.getSubExtractors();
			int[] in = new int[el.size()];
			int n = 0;
			for(BinaryExtractor<?> e: el) {
				in[n++] = addExtractor(sourceId, e);
			}
			CompositionToken ct = new CompositionToken();
			ct.extractorType = extractor.getClass();
			ct.inArgs = in;
			ct.operationToken = ce.getOperationToken();
			
			Integer node = compositionIndex.get(ct);
			if (node != null) {
				return node;
			}
			else {
				int id = nodes.size();
				Composition c = new Composition(ce);
				c.id = id;
				c.outIndex = id;
				compositionIndex.put(ct, id);
				nodes.add(c);
				for(int i = 0; i != in.length; ++i) {
					addLink(in[i], new CompositionLink(id, i));
				}
				return id;				
			}
		}
		else {
			List<Batch> bl = batches.get(sourceId);
			if (bl == null) {
				batches.put(sourceId, bl = new ArrayList<Batch>());
			}
			
			for(Batch batch: bl) {
				if (extractor.isCompatible(batch.extractorSet)) {
					return addToBatch(extractor, batch);
				}
			}
			// create new batch
			Batch batch = new Batch();
			bl.add(batch);
			batch.extractors = add(batch.extractors, extractor);
			batch.extractorSet = extractor.newExtractorSet();
			batch.id = nodes.size();
			nodes.add(batch);
			addLink(sourceId, new CompositionLink(batch.id, 0));

			return addToBatch(extractor, batch);
		}
	}

	private int addToBatch(BinaryExtractor<?> extractor, Batch batch) {
		int x = batch.extractorSet.addExtractor(extractor);
		batch.extractors = add(batch.extractors, extractor);
		if (batch.outIndexes.get(x) == Int2Int.NOT_SET) {
			int id = nBatchResult--;
			batch.outIndexes.set(x, id);
		}
		return batch.outIndexes.get(x);
	}

	@SuppressWarnings("rawtypes")
	private BinaryExtractor<?>[] add(BinaryExtractor<?>[] extractors, BinaryExtractor<?> extractor) {
		if (extractors == null) {
			BinaryExtractor<?>[] r = new BinaryExtractor[1];
			r[0] = (BinaryExtractor) extractor;
			return r;
		}
		else {
			extractors = Arrays.copyOf(extractors, extractors.length + 1);
			extractors[extractors.length - 1] = extractor;
			return extractors;
		}
	}

	private void addLink(int index, ValueLink link) {
		if (links.containsKey(index)) {
			links.put(index, new ForkLink(links.get(index), link));
		}
		else {
			links.put(index, link);
		}
	}

	@Override
	public int getSize() {
		return outs.size();
	}

	@Override
	public void compile() {
		if (!compiled) { 
			compiled = true;
			for(ProcessingNode node: nodes) {
				node.compile();
			}
		}
	}
	
	public void dump(StringBuilder builder) {
		if (!compiled) {
			throw new IllegalStateException("Should be compiled");
		}
		builder.append("<composite>\n");
		for(ProcessingNode node: nodes) {
			dumpNode(builder, node);
		}
		builder.append("</composite>");
	}
	
	private void dumpNode(StringBuilder builder, ProcessingNode node) {
		if (node instanceof Batch) {
			Batch batch = (Batch) node;
			builder.append("<batch id=\"B" + batch.id + "\">\n");
			batch.extractorSet.dump(builder);
			for(int i = 0; i != batch.outLinks.length; ++i) {
				if (batch.outLinks[i] != null) {
					dumpLink(builder, i, batch.outLinks[i]);
				}
			}
			builder.append("</batch>\n");
		}
		else {
			Composition c = (Composition)node;
			builder.append("<composition id=\"C" + c.id + "\">\n");
			builder.append("<extractor>").append(c.extractor).append("</extractor>\n");
			dumpLink(builder, Integer.MIN_VALUE, c.outLink);
			builder.append("</composition>\n");
		}
	}
	
	private void dumpLink(StringBuilder builder, int i, ValueLink valueLink) {
		if (valueLink instanceof ForkLink) {
			ForkLink fl = (ForkLink) valueLink;
			dumpLink(builder, i, fl.a);
			dumpLink(builder, i, fl.b);
		}
		else {
			if (i != Integer.MIN_VALUE) {
				builder.append("<link n=\"" + i + "\">");
			}
			else {
				builder.append("<link>");
			}
			if (valueLink instanceof ResultVectorLink) {
				builder.append("R" + ((ResultVectorLink)valueLink).outIndex);
			}			
			if (valueLink instanceof CompositionLink) {
				CompositionLink cl = (CompositionLink) valueLink;
				if (nodes.get(cl.id) instanceof Batch) {
					builder.append("B" + cl.id);
				}
				else {
					builder.append("C" + cl.id + "[" + cl.argIndex + "]");
				}
			}			
			builder.append("</link>\n");
		}
	}

	@Override
	public void extractAll(Object source, VectorResultReceiver resultReceiver) {
		if (!compiled) {
			throw new IllegalStateException("Extractor set is not compiled");
		}
		ExtractionContext context = newContext(resultReceiver);
		new LinkRef(links.get(-1), context).push(source);
		
		for(Composer composer: context.composers) {
			composer.compose(context);
		}
	}
	
	private ExtractionContext newContext(VectorResultReceiver resultVector) {
		if (nodes.isEmpty()) {
			return new ExtractionContext(resultVector, Collections.<Composer>emptyList());
		}
		else {
			List<Composer> composers = new ArrayList<Composer>(nodes.size());
			for(ProcessingNode c: nodes) {
				composers.add(c.newComposer());
			}
			return new ExtractionContext(resultVector, composers);
		}
	}
	
	private static class ExtractionContext {
		
		final VectorResultReceiver resultVector;
		final List<Composer> composers;
		
		private ExtractionContext(VectorResultReceiver resultVector, List<Composer> composers) {
			this.resultVector = resultVector;
			this.composers = composers;
		}
	}
	
	private static class CompositionToken {
		
		Class<?> extractorType;
		Object operationToken;
		int[] inArgs;
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((extractorType == null) ? 0 : extractorType.hashCode());
			result = prime * result + Arrays.hashCode(inArgs);
			result = prime
					* result
					+ ((operationToken == null) ? 0 : operationToken.hashCode());
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
			CompositionToken other = (CompositionToken) obj;
			if (extractorType == null) {
				if (other.extractorType != null)
					return false;
			} else if (!extractorType.equals(other.extractorType))
				return false;
			if (!Arrays.equals(inArgs, other.inArgs))
				return false;
			if (operationToken == null) {
				if (other.operationToken != null)
					return false;
			} else if (!operationToken.equals(other.operationToken))
				return false;
			return true;
		}
	}
	
	private abstract class Composer implements VectorResultReceiver {
		
		public abstract void compose(ExtractionContext context);
		
	}
	
	private abstract class ProcessingNode {

		int id;

		public abstract void compile(); 

		public abstract Composer newComposer();
		
	}

	private class Batch extends ProcessingNode {
		
		BinaryExtractor<?>[] extractors;
		Int2Int outIndexes = new Int2Int(); 
		ValueLink[] outLinks;
		BinaryExtractorSet extractorSet;

		@Override
		public void compile() {
			extractorSet.compile();
			outLinks = new ValueLink[outIndexes.size()];
			for(int i = 0; i != outIndexes.size(); ++i) {
				outLinks[i] = links.get(outIndexes.get(i));
			}
		}

		@Override
		public Composer newComposer() {
			return new ExtractorSetComposer();
		}

		private class ExtractorSetComposer extends Composer {
			
			boolean set = false;
			boolean done = false;
			Object value;
			
			
			@Override
			public void push(int id, Object part) {
				if (done) {
					throw new IllegalStateException("Already calculated");
				}
				if (id != 0) {
					throw new IllegalArgumentException("Input param [" + id + "] is unexpected");
				}
				if (set) {
					throw new IllegalStateException("Input param [0] is already set");
				}
				set = true;
				value = part;
			}

			public void compose(final ExtractionContext context) {
				done = true;
				extractorSet.extractAll(value, new VectorResultReceiver() {
					@Override
					public void push(int id, Object part) {
						outLinks[id].push(context, part);
					}
				});
			}
		}
	}
	
	private class Composition extends ProcessingNode {
		
		final CompositeExtractor<?> extractor;
		int outIndex;
		ValueLink outLink;
		
		Composition(CompositeExtractor<?> extractor) {
			this.extractor = extractor;
		}

		@Override
		public void compile() {
			outLink = links.get(outIndex);
		}		

		@Override
		public Composer newComposer() {
			return new FunctionComposer(extractor.newComposer());
		}

		private class FunctionComposer extends Composer {
			
			private ValueComposer composer;
			private boolean done = false;
			
			public FunctionComposer(ValueComposer composer) {
				this.composer = composer;
			}

			@Override
			public void push(int id, Object part) {
				if (done) {
					throw new IllegalStateException("Already calculated");
				}
				composer.push(id, part);
			}

			@Override
			public void compose(ExtractionContext context) {
				done = true;
				composer.compose(new LinkRef(outLink, context));
			}
		}
	}
	
	private static interface ValueLink {
		public void push(ExtractionContext context, Object value);
	}
	
	private static class LinkRef implements ScalarResultReceiver {
		
		private final ValueLink link;
		private final ExtractionContext context;

		public LinkRef(ValueLink link, ExtractionContext context) {
			this.link = link;
			this.context = context;
		}

		@Override
		public void push(Object part) {
			link.push(context, part);
		}

		@Override
		public String toString() {
			return link.toString();
		}
	}
	
	private static class ForkLink implements ValueLink {
		
		private final ValueLink a;
		private final ValueLink b;
		
		private ForkLink(ValueLink a, ValueLink b) {
			this.a = a;
			this.b = b;
		}

		@Override
		public void push(ExtractionContext context, Object value) {
			a.push(context, value);
			b.push(context, value);			
		}
	}
	
	private static class ResultVectorLink implements ValueLink {
		
		final int outIndex;
		
		public ResultVectorLink(int outIndex) {
			this.outIndex = outIndex;
		}

		@Override
		public void push(ExtractionContext context, Object value) {
			context.resultVector.push(outIndex, value);
		}
		
		@Override
		public String toString() {
			return "R[" + outIndex + "]";
		}
	}

	private static class CompositionLink implements ValueLink {
		
		final int id;
		final int argIndex;

		CompositionLink(int id, int argIndex) {
			this.id = id;
			this.argIndex = argIndex;
		}

		@Override
		public void push(ExtractionContext context, Object value) {
			context.composers.get(id).push(argIndex, value);
		}		
		
		@Override
		public String toString() {
			return "C" + id + "[" + argIndex + "]";
		}
	}
}
