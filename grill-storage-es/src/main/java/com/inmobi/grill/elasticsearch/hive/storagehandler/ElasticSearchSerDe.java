package com.inmobi.grill.elasticsearch.hive.storagehandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hive.serde.serdeConstants.*;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ElasticSearchSerDe implements SerDe {
	private static boolean CONSIDER_TYPE = false;
	private ObjectInspector cachedObjectInspector;
	private List<String> columnNames = null;

	public ElasticSearchSerDe() throws SerDeException {
		super();
	}

	@Override
	public void initialize(Configuration job, Properties tbl)
			throws SerDeException {

		String columnNameProperty = tbl.getProperty(LIST_COLUMNS);
		String columnTypeProperty = tbl
				.getProperty(LIST_COLUMN_TYPES);
		columnNames = Arrays.asList(columnNameProperty.split(","));
		List<TypeInfo> columnTypes = TypeInfoUtils
				.getTypeInfosFromTypeString(columnTypeProperty);
		int numColumns = columnNames.size();
		// Create the ObjectInspectors for the fields.
		List<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
				numColumns);
		ObjectInspector colObjectInspector;
		for (int col = 0; col < numColumns; col++) {
			colObjectInspector = TypeInfoUtils
					.getStandardJavaObjectInspectorFromTypeInfo(columnTypes
							.get(col));
			columnObjectInspectors.add(colObjectInspector);
		}

		cachedObjectInspector = ObjectInspectorFactory
				.getColumnarStructObjectInspector(columnNames,
						columnObjectInspectors);

	}

	@SuppressWarnings("rawtypes")
	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {

		if (objInspector.getCategory() != org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT)
			throw new SerDeException((new StringBuilder())
					.append(getClass().toString())
					.append(" can only serialize struct types, but we got: ")
					.append(objInspector.getTypeName()).toString());
		StructObjectInspector soi = (StructObjectInspector) objInspector;
		List<Object> list = soi.getStructFieldsDataAsList(obj);

		MapWritable returnValue = new MapWritable();

		// printAll(fields);
		// printAll(list);
		int index = -1;
		for (Object eachValue : list) {
			index++;
			if (eachValue != null
					&& !eachValue.toString().equalsIgnoreCase("null")) {

				if (CONSIDER_TYPE) {
					if (eachValue instanceof Writable)
						returnValue.put(new Text(columnNames.get(index)),
								(Writable) eachValue);
					else if (eachValue instanceof LazyPrimitive) {
						returnValue
								.put(new Text(columnNames.get(index)),
										((LazyPrimitive) eachValue)
												.getWritableObject());
					} else if (eachValue instanceof Long) {
						returnValue
								.put(new Text(columnNames.get(index)),
										new LongWritable(((Long) eachValue)
												.longValue()));
					} else if (eachValue instanceof Integer) {
						returnValue.put(
								new Text(columnNames.get(index)),
								new IntWritable(((Integer) eachValue)
										.intValue()));
					} else if (eachValue instanceof Double) {
						returnValue.put(
								new Text(columnNames.get(index)),
								new DoubleWritable(((Double) eachValue)
										.doubleValue()));
					} else if (eachValue instanceof String) {
						returnValue.put(new Text(columnNames.get(index)),
								new Text(eachValue.toString()));
					} else {
						throw new SerDeException("Unknow data type "
								+ columnNames.get(index) + ":"
								+ eachValue.getClass());
					}

				} else {
					returnValue.put(new Text(columnNames.get(index)), new Text(
							eachValue.toString()));

				}
			}

		}

		return returnValue;
	}

	@Override
	public Object deserialize(Writable writable) throws SerDeException {
		throw new RuntimeException("Should not call deserialize");
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {

		return cachedObjectInspector;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return MapWritable.class;
	}
}
