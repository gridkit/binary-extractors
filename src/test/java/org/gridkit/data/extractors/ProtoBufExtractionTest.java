package org.gridkit.data.extractors;

import java.util.Arrays;

import org.gridkit.data.extractors.common.BinaryExtractor;
import org.gridkit.data.extractors.common.BinaryFilterExtractor;
import org.gridkit.data.extractors.common.Blob;
import org.gridkit.data.extractors.common.ChainedBinaryExtractor;
import org.gridkit.data.extractors.common.ConstExtractor;
import org.gridkit.data.extractors.common.EqualsPredicate;
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

		ChainedBinaryExtractor.chain()
							  .chain(ProtoBufExtractor.path(2))
							  .chain(ProtoBufExtractor.path(2, 3))
		                      .chain(ProtoBufExtractor.string(5));
		
		ProtoBufExtractor<String> se = ProtoBufExtractor.string(2,2,3,5);
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
		
		BinaryFilterExtractor<String> keyAFilter = new BinaryFilterExtractor<String>(keyAPred, valueField);
		BinaryFilterExtractor<String> keyBFilter = new BinaryFilterExtractor<String>(keyBPred, valueField);
		
		
		addExtractor("get(A)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), keyAFilter));
		addExtractor("get(B)", ChainedBinaryExtractor.chain(ProtoBufExtractor.path(1), keyBFilter));
		extract(getBytes("protobuf/TextProperties-1.bin"));
		assertValue("get(A)", "aaa");
		assertValue("get(B)", "bbb");
	}

	@Test
	public void extract_multiple_property_values_name() {
		ProtoBufExtractor<String> keyField = ProtoBufExtractor.string(1);
		ProtoBufExtractor<String> valueField = ProtoBufExtractor.string(2);
		
		BinaryExtractor<Boolean> keyAPred = new EqualsPredicate(keyField, ConstExtractor.newConst("A"));
		BinaryExtractor<Boolean> keyBPred = new EqualsPredicate(keyField, ConstExtractor.newConst("B"));
		BinaryExtractor<Boolean> keyXXPred = new EqualsPredicate(keyField, ConstExtractor.newConst("XX"));
		
		BinaryFilterExtractor<String> propAFilter = new BinaryFilterExtractor<String>(keyAPred, valueField);
		BinaryFilterExtractor<String> propBFilter = new BinaryFilterExtractor<String>(keyBPred, valueField);
		BinaryFilterExtractor<String> propXXFilter = new BinaryFilterExtractor<String>(keyXXPred, valueField);
		
		
		addExtractor("get(A)", ChainedBinaryExtractor.chain(ProtoBufExtractor.newBinaryExtractor(1), propAFilter));
		addExtractor("get(B)", ChainedBinaryExtractor.chain(ProtoBufExtractor.newBinaryExtractor(1), propBFilter));
		addExtractor("getAll(XX)", ListCollector.wrap(ChainedBinaryExtractor.chain(ProtoBufExtractor.newBinaryExtractor(1), propXXFilter)));
		extract(getBytes("protobuf/TextProperties-2.bin"));
		assertValue("get(A)", "aaa");
		assertValue("get(B)", "bbb");
		assertValue("getAll(XX)", Arrays.asList("v1", "v2", "v3", "v4", "v5"));
	}
}
