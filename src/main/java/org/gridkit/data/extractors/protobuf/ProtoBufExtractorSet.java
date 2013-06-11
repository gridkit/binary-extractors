package org.gridkit.data.extractors.protobuf;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.gridkit.data.extractors.common.BinaryExtractor;
import org.gridkit.data.extractors.common.BinaryExtractorSet;
import org.gridkit.data.extractors.common.BinaryReader;
import org.gridkit.data.extractors.common.CompositeExtractorSet;
import org.gridkit.data.extractors.common.VectorResultReceiver;
import org.gridkit.data.extractors.protobuf.ProtoBufExtractor.Encoding;

public class ProtoBufExtractorSet implements BinaryExtractorSet {

	private int numExtractors;
	private Entry root = new Entry(null, 0);
	
	@Override
	public int addExtractor(BinaryExtractor<?> extractor) {
		return root.addExtractor((ProtoBufExtractor<?>) extractor);
	}
	
	@Override
	public void compile() {
		root.prepare();		
	}

	@Override
	public int getSize() {
		return numExtractors;
	}

	@Override
	public void extractAll(ByteBuffer buffer, VectorResultReceiver resultReceiver) {
		try {
			ProtoBufCodedStream cis = PBHelper.inputStream(buffer);
			root.extractAll(cis, resultReceiver);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void dump(StringBuilder builder) {
		builder.append("<protobuf-extractor>\n");
		root.dump(builder);
		builder.append("</protobuf-extractor>\n");
	}



	private class Entry implements Serializable, Comparable<Entry> {
		
		private static final long serialVersionUID = 20130127L;
		
		@SuppressWarnings("unused")
		private final Entry parent;
		private final int pbIndex;
		
		private Map<Integer, Entry> childEntries = new TreeMap<Integer, Entry>();
		private List<PrimitiveSlot> primitiveSlots = new ArrayList<PrimitiveSlot>();
		private CompositeExtractorSet composite;
		private Int2Int compositeMapping;
		
		public Entry(Entry parent, int pbIndex) {
			this.parent = parent;
			this.pbIndex = pbIndex;
		}
		
		@Override
		public int compareTo(Entry o) {
			return pbIndex - o.pbIndex;
		} 
		
		public int addExtractor(ProtoBufExtractor<?> extractor) {
			if (extractor.isLeaf()) {
				if (extractor.getNestedExtractor() == null) {
					throw new IllegalArgumentException("Primitive ProtoBuf extractor cannot be root");
				}
				else {
					if (composite == null) {
						composite = new CompositeExtractorSet();
						compositeMapping = new Int2Int();
					}
					int cid = composite.addExtractor(extractor.getNestedExtractor());
					if (compositeMapping.get(cid) >= 0) {
						return compositeMapping.get(cid);
					}
					else {
						int id = numExtractors++;
						compositeMapping.set(cid, id);
						return id;
					}
				}
			}
			else {
				int pbi = extractor.getPrefix();
				if (extractor.getPath().length == 1 && extractor.getEncoding() != null) {
					Encoding enc = extractor.getEncoding();
					PrimitiveSlot slot = getPrimitiveSlot(pbi, enc);
					if (slot.outputId == -1) {
						slot.outputId = numExtractors++;
					}
					return slot.outputId;
				}
				else {
					Entry child = getChild(pbi, true);
					return child.addExtractor(extractor.trim());
				}
			}
		}
		
		private PrimitiveSlot getPrimitiveSlot(int pbi, Encoding enc) {
			PrimitiveSlot slot = new PrimitiveSlot(pbi, enc);
			int n = Collections.binarySearch(primitiveSlots, slot);
			if (n >= 0) {
				return primitiveSlots.get(n);
			}
			else {
				primitiveSlots.add(slot);
				Collections.sort(primitiveSlots);
			}					
			return slot;
		}

		private List<PrimitiveSlot> getPrimitiveSlots(int pbi) {
			PrimitiveSlot low = new PrimitiveSlot(pbi, Encoding.values()[0]);
			int l = Collections.binarySearch(primitiveSlots, low);
			if (l < 0) {
				l = -(l + 1);				
			}
			for(int n = l; n != primitiveSlots.size(); ++n) {
				if (primitiveSlots.get(n).pbid != pbi) {
					return primitiveSlots.subList(l, n);
				}
			}
			return primitiveSlots.subList(l, primitiveSlots.size());
		}

		private Entry getChild(int pbi, boolean create) {
			Entry entry = childEntries.get(pbi);
			if (entry == null && create) {
				entry = new Entry(this, pbi);
				childEntries.put(pbi, entry);
			}
			return entry;
		}

		public void prepare() {
			if (composite != null) {
				composite.compile();
			}
			if (childEntries != null) {
				for(Entry child: childEntries.values()) {
					child.prepare();
				}
			}
		}
		
		public void extractAll(ProtoBufCodedStream cis, VectorResultReceiver receiver) throws IOException {
			if (composite != null) {
				BinaryReader br = cis.limitedReader();
				composite.extractAll(br.asBuffer(), compositeMapping.newMapper(receiver));
			}
			if (primitiveSlots.isEmpty() && childEntries.isEmpty()) {
				// do not parse fields
				cis.skipRawBytes(cis.getBytesUntilLimit());
				return;
			}
			while(!cis.isAtEnd()) {
				int tag = cis.readTag();
				int pbId = ProtoBufCodedStream.getTagFieldNumber(tag);
				int type = tag & 7;
				switch(type) {
				case ProtoBufCodedStream.WIRETYPE_VARINT:
				case ProtoBufCodedStream.WIRETYPE_FIXED32:
				case ProtoBufCodedStream.WIRETYPE_FIXED64:
				{
					BinaryReader reader = cis.limitedReader();
					int off = cis.getTotalBytesRead();
					cis.skipField(tag);
					int len = cis.getTotalBytesRead() - off;
					reader = reader.slice(0, len);
					processPrimitives(pbId, type, reader, receiver);
					if (childEntries.containsKey(pbId)) {
						childEntries.get(pbId).extractAll(PBHelper.inputStream(reader.asBuffer()), receiver);						
					}
					break;
				}
				case ProtoBufCodedStream.WIRETYPE_LENGTH_DELIMITED:
				{					
					int len = cis.readUInt32();
					BinaryReader reader = cis.limitedReader();
 					reader = reader.slice(0, len);
					processPrimitives(pbId, type, reader, receiver);
					if (childEntries.containsKey(pbId)) {
						int limit = cis.pushLimit(len);
						childEntries.get(pbId).extractAll(cis, receiver);
						cis.skipMessage();
						cis.popLimit(limit);
					}
					else {
						cis.skipRawBytes(len);
					}
					break;
				}	
				default: 
					throw new IllegalArgumentException("Unknown wire type " + type);
				}
			}
		}
		
		public void dump(StringBuilder builder) {
			if (composite != null) {
				builder.append("<extractor-set>\n");
				composite.dump(builder);
				builder.append("</extractor-set>\n");
				for(int i = 0; i != compositeMapping.size(); ++i) {
					if (compositeMapping.get(i) >= 0) {
						builder.append("<link id=\"" + i + "\" outIndex=\"" + compositeMapping.get(i)+ "\"/>\n");
					}
				}
			}
			if (!primitiveSlots.isEmpty() || !childEntries.isEmpty()) {
				builder.append("<fields>\n");
				for(PrimitiveSlot slot: primitiveSlots) {
					String type = slot.encoding.toString().toLowerCase();
					builder.append("<" + type + " fid=\"" + slot.pbid + "\" outIndex=\"" + slot.outputId + "\"/>\n");
				}
				for(Entry child: childEntries.values()) {
					builder.append("<message fid=\"" + child.pbIndex + "\">\n");
					child.dump(builder);
					builder.append("</message>\n");
				}				
				builder.append("</fields>\n");
			}
		}

		private void processPrimitives(int pbId, int type, BinaryReader reader, VectorResultReceiver receiver) throws IOException {
			ByteBuffer target = null;
			for(PrimitiveSlot slot: getPrimitiveSlots(pbId)) {
				if (target == null) {
					target = reader.asBuffer();
				}
				receiver.push(slot.outputId, slot.encoding.decode(type, target));
			}			
		}
	}
	
	private static class PrimitiveSlot implements Comparable<PrimitiveSlot> {
		
		private final int pbid;
		private final ProtoBufExtractor.Encoding encoding;
		private int outputId = -1;
		
		PrimitiveSlot(int pbid, Encoding encoding) {
			this.pbid = pbid;
			this.encoding = encoding;
		}

		@Override
		public int compareTo(PrimitiveSlot o) {
			int c = pbid - o.pbid;
			if (c == 0) {
				return encoding.ordinal() - o.encoding.ordinal(); 
			}
			else {
				return c;
			}
		}
	}		
}
