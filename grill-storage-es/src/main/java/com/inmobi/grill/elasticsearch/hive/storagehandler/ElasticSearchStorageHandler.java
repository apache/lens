package com.inmobi.grill.elasticsearch.hive.storagehandler;

import java.util.Map;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.OutputFormat;

import com.inmobi.grill.elasticsearch.outputformat.ElasticSearchOutputFormat;

public class ElasticSearchStorageHandler extends DefaultStorageHandler {

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {

		return ElasticSearchOutputFormat.class;

	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return ElasticSearchSerDe.class;
	}

	@Override
	public void configureInputJobProperties(TableDesc tabledesc,
			Map<String, String> map) {
		//System.out.println("configureInputJobProperties");
		configureTableJobProperties(tabledesc, map);
	}

	@Override
	public void configureOutputJobProperties(TableDesc tabledesc,
			Map<String, String> map) {
		configureTableJobProperties(tabledesc, map);
	}

	@Override
	public void configureTableJobProperties(TableDesc tabledesc,
			Map<String, String> map) {

		map.put(ElasticSearchOutputFormat.TABLE_NAME,tabledesc.getTableName());
		super.configureTableJobProperties(tabledesc, map);
	}
}
