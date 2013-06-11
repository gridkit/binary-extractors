package org.gridkit.data.extractors;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.gridkit.data.extractors.common.AbstractValueTransformer;
import org.gridkit.data.extractors.common.BinaryExtractor;
import org.gridkit.data.extractors.common.FilterExtractor;
import org.gridkit.data.extractors.common.Blob;
import org.gridkit.data.extractors.common.ChainedBinaryExtractor;
import org.gridkit.data.extractors.common.ComparisonPredicate;
import org.gridkit.data.extractors.common.ConstExtractor;
import org.gridkit.data.extractors.common.EqualsPredicate;
import org.gridkit.data.extractors.common.Extractors;
import org.gridkit.data.extractors.common.ListCollector;
import org.gridkit.data.extractors.protobuf.ProtoBufExtractor;
import org.junit.Test;

public class ProtoBufExtractionTest extends BaseExtractionAssertTest {
	
	@Test
	public void extract_unsigned_int_from_simple_object() {
		ProtoBufExtractor<Integer> pbe = ProtoBufExtractor.int32(1);
		addExtractor("intField", pbe);
		extract(getBytes("protobuf/SimpleObject-1.bin"));
		assertValue("intField", 128);
	}

	@Test
	public void extract_signed_int_from_simple_object() {
		ProtoBufExtractor<Integer> pbe = ProtoBufExtractor.sint32(2);
		addExtractor("sintField", pbe);
		extract(getBytes("protobuf/SimpleObject-1.bin"));
		assertValue("sintField", -100);
	}

	@Test
	public void extract_unsigned_long_from_simple_object() {
		ProtoBufExtractor<Long> pbe = ProtoBufExtractor.int64(3);
		addExtractor("longField", pbe);
		extract(getBytes("protobuf/SimpleObject-1.bin"));
		assertValue("longField", 10000000000l);
	}

	@Test
	public void extract_signed_long_from_simple_object() {
		ProtoBufExtractor<Long> pbe = ProtoBufExtractor.sint64(4);
		addExtractor("slongField", pbe);
		extract(getBytes("protobuf/SimpleObject-1.bin"));
		assertValue("slongField", -10000000000l);
	}

	@Test
	public void extract_string_from_simple_object() {
		ProtoBufExtractor<String> pbe = ProtoBufExtractor.string(5);
		addExtractor("stringField", pbe);
		extract(getBytes("protobuf/SimpleObject-1.bin"));
		assertValue("stringField", "ABC");
	}

	@Test
	public void extract_double_from_simple_object() {
		ProtoBufExtractor<Number> pbe = ProtoBufExtractor.fp(6);
		addExtractor("doubleField", pbe);
		extract(getBytes("protobuf/SimpleObject-1.bin"));
		assertValue("doubleField", 3.14d);
	}

	@Test
	public void extract_float_from_simple_object() {
		ProtoBufExtractor<Number> pbe = ProtoBufExtractor.fp(7);
		addExtractor("floatField", pbe);
		extract(getBytes("protobuf/SimpleObject-1.bin"));
		assertValue("floatField", 3.14f);
	}

	@Test
	public void extract_blob_from_simple_object() {
		ProtoBufExtractor<Blob> pbe = ProtoBufExtractor.newBlobExtractor(8);
		addExtractor("blobField", pbe);
		extract(getBytes("protobuf/SimpleObject-1.bin"));
		assertValue("blobField", blob("XYZ"));
	}
	
	@Test
	public void extract_two_interpretation_of_int() {
		ProtoBufExtractor<Integer> pbes = ProtoBufExtractor.sint32(2);
		ProtoBufExtractor<Integer> pbeu = ProtoBufExtractor.int32(2);
		addExtractor("signed", pbes);
		addExtractor("unsigned", pbeu);
		extract(getBytes("protobuf/SimpleObject-1.bin"));
		assertValue("signed", -100);
		assertValue("unsigned", 199);
	}
	
	@Test
	public void extract_from_tree() {
		ProtoBufExtractor<Integer> ie = ProtoBufExtractor.int32(1,1,3,1);
		addExtractor("l/l/v/int", ie);

		BinaryExtractor<String> se = ChainedBinaryExtractor.chain()
							  .chain(ProtoBufExtractor.path(2))
							  .chain(ProtoBufExtractor.path(2, 3))
		                      .chain(ProtoBufExtractor.string(5));
		
		addExtractor("r/r/v/string", se);
		extract(getBytes("protobuf/Tree-1.bin"));
		assertValue("l/l/v/int", 1000);
		assertValue("r/r/v/string", "Abc");
	}
	
	@Test
	public void extract_key_set_from_properties() {
		ProtoBufExtractor<String> keyPath = ProtoBufExtractor.string(1,1);
		ListCollector<String> collector = new ListCollector<String>(keyPath);
		addExtractor("keySet", collector);
		extract(getBytes("protobuf/TextProperties-1.bin"));
		assertValue("keySet", Arrays.asList("A", "B", "C", "D"));
	}

	@Test
	public void extract_property_by_name() {
		ProtoBufExtractor<String> keyField = ProtoBufExtractor.string(1);
		ProtoBufExtractor<String> valueField = ProtoBufExtractor.string(2);

		BinaryExtractor<Boolean> keyAPred = new EqualsPredicate(keyField, ConstExtractor.newConst("A"));
		BinaryExtractor<Boolean> keyBPred = new EqualsPredicate(keyField, ConstExtractor.newConst("B"));
		
		FilterExtractor<String> keyAFilter = new FilterExtractor<String>(keyAPred, valueField);
		FilterExtractor<String> keyBFilter = new FilterExtractor<String>(keyBPred, valueField);
		
		
		addExtractor("get(A)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), keyAFilter));
		addExtractor("get(B)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), keyBFilter));
		extract(getBytes("protobuf/TextProperties-1.bin"));
		assertValue("get(A)", "aaa");
		assertValue("get(B)", "bbb");
	}

	@Test(expected=NumberFormatException.class)
	public void extract_property_by_name__fail_on_eager_parsing() {
		ProtoBufExtractor<String> keyField = ProtoBufExtractor.string(1);
		BinaryExtractor<Integer> valueField = new StringToInt(ProtoBufExtractor.string(2));
		
		BinaryExtractor<Boolean> keyAPred = new EqualsPredicate(keyField, ConstExtractor.newConst("A"));
		BinaryExtractor<Boolean> keyBPred = new EqualsPredicate(keyField, ConstExtractor.newConst("B"));
		
		FilterExtractor<Integer> keyAFilter = FilterExtractor.filter(keyAPred, valueField);
		FilterExtractor<Integer> keyBFilter = FilterExtractor.filter(keyBPred, valueField);
		
		
		addExtractor("get(A)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), keyAFilter));
		addExtractor("get(B)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), keyBFilter));
		extract(getBytes("protobuf/TextProperties-3.bin"));
		assertValue("get(A)", 128);
		assertValue("get(B)", 256);
	}

	@Test
	public void extract_property_by_name_using_lazy_parser() {
		ProtoBufExtractor<String> keyField = ProtoBufExtractor.string(1);
		BinaryExtractor<Integer> valueField = new StringToInt(ProtoBufExtractor.string(2));
		
		BinaryExtractor<Boolean> keyAPred = new EqualsPredicate(keyField, ConstExtractor.newConst("A"));
		BinaryExtractor<Boolean> keyBPred = new EqualsPredicate(keyField, ConstExtractor.newConst("B"));
		
		FilterExtractor<Integer> keyAFilter = FilterExtractor.lazyFilter(keyAPred, valueField);
		FilterExtractor<Integer> keyBFilter = FilterExtractor.lazyFilter(keyBPred, valueField);
		
		addExtractor("get(A)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), keyAFilter));
		addExtractor("get(B)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), keyBFilter));
		extract(getBytes("protobuf/TextProperties-3.bin"));
		assertValue("get(A)", 128);
		assertValue("get(B)", 256);
	}

	@Test
	public void extract_multiple_property_values_name() {
		ProtoBufExtractor<String> keyField = ProtoBufExtractor.string(1);
		ProtoBufExtractor<String> valueField = ProtoBufExtractor.string(2);
		
		BinaryExtractor<Boolean> keyAPred = new EqualsPredicate(keyField, ConstExtractor.newConst("A"));
		BinaryExtractor<Boolean> keyBPred = new EqualsPredicate(keyField, ConstExtractor.newConst("B"));
		BinaryExtractor<Boolean> keyXXPred = new EqualsPredicate(keyField, ConstExtractor.newConst("XX"));
		
		FilterExtractor<String> propAFilter = new FilterExtractor<String>(keyAPred, valueField);
		FilterExtractor<String> propBFilter = new FilterExtractor<String>(keyBPred, valueField);
		FilterExtractor<String> propXXFilter = new FilterExtractor<String>(keyXXPred, valueField);
		
		
		addExtractor("get(A)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), propAFilter));
		addExtractor("get(B)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), propBFilter));
		addExtractor("getAll(XX)", ListCollector.wrap(ChainedBinaryExtractor.chain(ProtoBufExtractor.newBinaryExtractor(1), propXXFilter)));
		extract(getBytes("protobuf/TextProperties-2.bin"));
		assertValue("get(A)", "aaa");
		assertValue("get(B)", "bbb");
		assertValue("getAll(XX)", Arrays.asList("v1", "v2", "v3", "v4", "v5"));
	}

	@Test
	public void extract_prop_range() {
		ProtoBufExtractor<String> keyField = ProtoBufExtractor.string(1);
		ProtoBufExtractor<String> valueField = ProtoBufExtractor.string(2);
		
		// This will select key below "C" lexicographically 
		BinaryExtractor<Boolean> keyBelowCPred = new ComparisonPredicate(ComparisonPredicate.Op.LT, keyField, ConstExtractor.newConst("C"));
				
		addExtractor("getAll([...,C))", ListCollector.wrap(
				ChainedBinaryExtractor.chain()
					.chain(ProtoBufExtractor.newBinaryExtractor(1))
					.chain(Extractors.filter(keyBelowCPred))
					.chain(valueField)));
		
		extract(getBytes("protobuf/TextProperties-2.bin"));
		assertValue("getAll([...,C))", Arrays.asList("aaa", "bbb"));
	}

	@Test
	public void extract_using_custom_parser() {
		ProtoBufExtractor<String> keyField = ProtoBufExtractor.string(1);
		BinaryExtractor<Long> valueField = ChainedBinaryExtractor.chain().chain(ProtoBufExtractor.path(2)).chain(new BlobLength());
		
		BinaryExtractor<Boolean> keyAPred = new EqualsPredicate(keyField, ConstExtractor.newConst("A"));
		BinaryExtractor<Boolean> keyBPred = new EqualsPredicate(keyField, ConstExtractor.newConst("B"));
		BinaryExtractor<Boolean> keyXXPred = new EqualsPredicate(keyField, ConstExtractor.newConst("XX"));
		
		FilterExtractor<Long> propAFilter = new FilterExtractor<Long>(keyAPred, valueField);
		FilterExtractor<Long> propBFilter = new FilterExtractor<Long>(keyBPred, valueField);
		FilterExtractor<Long> propXXFilter = new FilterExtractor<Long>(keyXXPred, valueField);
		
		addExtractor("get(A)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), propAFilter));
		addExtractor("get(B)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), propBFilter));
		addExtractor("getAll(XX)", ListCollector.wrap(ChainedBinaryExtractor.chain(ProtoBufExtractor.newBinaryExtractor(1), propXXFilter)));
		extract(getBytes("protobuf/TextProperties-2.bin"));
		assertValue("get(A)", 3l);
		assertValue("get(B)", 3l);
		assertValue("getAll(XX)", Arrays.asList(2l, 2l, 2l, 2l, 2l));
	}
	
	@SuppressWarnings("serial")
	public static class BlobLength extends AbstractValueTransformer<ByteBuffer, Long> {

		public BlobLength() {
			super();
		}

		public BlobLength(BinaryExtractor<ByteBuffer> sourceExtractor) {
			super(sourceExtractor);
		}

		@Override
		protected Long transform(ByteBuffer input) {
			return (long)input.remaining();
		}

		@Override
		public String toString() {
			return super.toString() + "BlobLenght";
		}
	}

	@SuppressWarnings("serial")
	public static class StringToInt extends AbstractValueTransformer<String, Integer> {
		
		public StringToInt() {
			super();
		}
		
		public StringToInt(BinaryExtractor<String> sourceExtractor) {
			super(sourceExtractor);
		}
		
		@Override
		protected Integer transform(String input) {
			return Integer.parseInt(input);
		}
		
		@Override
		public String toString() {
			return super.toString() + "StringToInt";
		}
	}
}
