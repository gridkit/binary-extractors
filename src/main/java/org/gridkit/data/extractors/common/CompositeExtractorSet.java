package org.gridkit.data.extractors.common;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gridkit.data.extractors.common.CompositeExtractor.ValueComposer;

public class CompositeExtractorSet implements BinaryExtractorSet, Serializable {

	private static final long serialVersionUID = 20130127L;
	
	private List<Batch> batches = new ArrayList<Batch>();
	private List<Composition> compositions = new ArrayList<Composition>();
	private int nOut = 0;
	private int nInternal = 0;
	
	private Map<Integer, ValueLink> links = new HashMap<Integer, ValueLink>();

	private boolean compiled;

	@Override
	public int addExtractor(BinaryExtractor<?> extractor) {
		if (compiled) {
			throw new IllegalStateException("Cannot add extractor to a compiled set");
		}
		int id = addExtractor(extractor, false);
		addLink(id, new ResultVectorLink(id));
		return id;
	}

	private int addExtractor(BinaryExtractor<?> extractor, boolean intermediate) {
		if (extractor instanceof CompositeExtractor) {
			int id = intermediate ? - (++nInternal) : nOut++;
			CompositeExtractor<?> ce = (CompositeExtractor<?>) extractor;
			List<BinaryExtractor<?>> el = ce.getSubExtractors();
			int[] in = new int[el.size()];
			int n = 0;
			for(BinaryExtractor<?> e: el) {
				in[n++] = addExtractor(e, true);
			}
			int cid = compositions.size();
			Composition c = new Composition(cid, ce);
			c.outIndex = id;
			compositions.add(c);
			for(int i = 0; i != in.length; ++i) {
				addLink(in[i], new CompositionLink(cid, i));
			}
			return id;
		}
		else {
			for(Batch batch: batches) {
				if (extractor.isCompatible(batch.extractorSet)) {
					int x = batch.extractorSet.addExtractor(extractor);
					batch.extractors = add(batch.extractors, extractor);
					if (batch.outIndexes.get(x) == Int2Int.NOT_SET) {
						int id = intermediate ? - (++nInternal) : nOut++;
						batch.outIndexes.set(x, id);
					}
					return batch.outIndexes.get(x);
				}
			}
			// create new batch
			Batch batch = new Batch();
			batches.add(batch);
			batch.extractors = add(batch.extractors, extractor);
			batch.extractorSet = extractor.newExtractorSet();

			int id = intermediate ? - (++nInternal) : nOut++;
			int x = batch.extractorSet.addExtractor(extractor);
			batch.outIndexes.set(x, id);
			return id;
		}
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
		return nOut;
	}

	@Override
	public void compile() {
		if (!compiled) { 
			compiled = true;
			for(Batch batch: batches) {
				batch.extractorSet.compile();
				batch.outLinks = new ValueLink[batch.outIndexes.size()];
				for(int i = 0; i != batch.outIndexes.size(); ++i) {
					batch.outLinks[i] = links.get(batch.outIndexes.get(i));
				}
			}
			for(Composition composition: compositions) {
				composition.outLink = links.get(composition.outIndex);
			}
		}
	}
	
	public void dump(StringBuilder builder) {
		if (!compiled) {
			throw new IllegalStateException("Should be compiled");
		}
		builder.append("<composite>\n");
		if (!batches.isEmpty()) {
			builder.append("<batches>\n");
				for(Batch batch: batches) {
					builder.append("<extractor-set>\n");
					batch.extractorSet.dump(builder);
					for(int i = 0; i != batch.outLinks.length; ++i) {
						if (batch.outLinks[i] != null) {
							dumpLink(builder, i, batch.outLinks[i]);
						}
					}
					builder.append("</extractor-set>\n");
				}
			builder.append("</batches>\n");
		}
		if (!compositions.isEmpty()) {
			builder.append("<compositions>\n");
			for(int i = 0; i != compositions.size(); ++i) {
				Composition c = compositions.get(i);
				builder.append("<composition n=\"" + i + "\">\n");
				builder.append("<extractor>").append(c.extractor).append("</extractor>\n");
				dumpLink(builder, Integer.MIN_VALUE, c.outLink);
				builder.append("</composition>\n");
			}			
			builder.append("</compositions>\n");
		}
		builder.append("</composite>");
	}
	
	private void dumpLink(StringBuilder builder, int i, ValueLink valueLink) {
		if (valueLink instanceof ForkLink) {
			ForkLink fl = (ForkLink) valueLink;
			dumpLink(builder, i, fl.a);
			dumpLink(builder, i, fl.b);
		}
		else {
			if (i != Integer.MIN_VALUE) {
				builder.append("<link n=\"" + i + "\">").append(valueLink).append("</link>\n");
			}
			else {
				builder.append("<link>").append(valueLink).append("</link>\n");
			}
		}
	}

	@Override
	public void extractAll(ByteBuffer buffer, VectorResultReceiver resultReceiver) {
		if (!compiled) {
			throw new IllegalStateException("Extractor set is not compiled");
		}
		ExtractionContext context = newContext(resultReceiver);
		
		for(Batch batch: batches) {
			batch.exec(buffer, context);
		}
		
		for(Composition c: compositions) {
			c.exec(context);
		}		
	}
	
	private ExtractionContext newContext(VectorResultReceiver resultVector) {
		if (compositions.isEmpty()) {
			return new ExtractionContext(resultVector, Collections.<ValueComposer>emptyList());
		}
		else {
			List<ValueComposer> composers = new ArrayList<CompositeExtractor.ValueComposer>(compositions.size());
			for(Composition c: compositions) {
				composers.add(c.extractor.newComposer());
			}
			return new ExtractionContext(resultVector, composers);
		}
	}
	
	private static class ExtractionContext {
		
		final VectorResultReceiver resultVector;
		final List<ValueComposer> composers;
		
		private ExtractionContext(VectorResultReceiver resultVector, List<ValueComposer> composers) {
			this.resultVector = resultVector;
			this.composers = composers;
		}
	}
	
	private static class Batch {
		
		BinaryExtractor<?>[] extractors;
		Int2Int outIndexes = new Int2Int(); 
		ValueLink[] outLinks;
		BinaryExtractorSet extractorSet;

		
		public void exec(ByteBuffer buffer, final ExtractionContext context) {
			extractorSet.extractAll(buffer, new VectorResultReceiver() {
				@Override
				public void push(int id, Object part) {
					outLinks[id].push(context, part);
				}
			});
		}
	}
	
	private static class Composition {
		
		final int id;
		final CompositeExtractor<?> extractor;
		int outIndex;
		ValueLink outLink;
		
		Composition(int id, CompositeExtractor<?> extractor) {
			this.id = id;
			this.extractor = extractor;
		}

		public void exec(final ExtractionContext context) {
			context.composers.get(id).compose(new LinkRef(outLink, context));
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
		
		private final int outIndex;
		
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
		
		private final int compositionId;
		private final int argIndex;

		private CompositionLink(int compositionId, int argIndex) {
			this.compositionId = compositionId;
			this.argIndex = argIndex;
		}

		@Override
		public void push(ExtractionContext context, Object value) {
			context.composers.get(compositionId).push(argIndex, value);
		}		
		
		@Override
		public String toString() {
			return "C" + compositionId + "[" + argIndex + "]";
		}
	}
}
