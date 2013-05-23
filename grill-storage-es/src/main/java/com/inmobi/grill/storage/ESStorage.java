package com.inmobi.grill.storage;



public class ESStorage { /*extends Storage {
  private String storageHandlerClass;

  private ESStorage() {
    super(TableType.MANAGED_TABLE);
  }

  public ESStorage(String storageHandlerClass,
      Map<String, String> serdeParameters,
      Map<String, String> tableParameters) {
    this();
    this.storageHandlerClass = storageHandlerClass;
    addTableProperty(META_TABLE_STORAGE, storageHandlerClass);
    if (serdeParameters != null) {
      this.serdeParameters.putAll(serdeParameters);
    }
    if (tableParameters != null) {
      addToTableParameters(tableParameters);
    }
  }

  public ESStorage(Table table) {
    super(TableType.MANAGED_TABLE);
    //TODO
  }

  public String getStorageHandlerClass() {
    return storageHandlerClass;
  }

  @Override
  public String getName() {
    return StorageConstants.ES_STORAGE_NAME;
  }

  @Override
  public void setSD(StorageDescriptor sd) throws BreezeException {
    try {
      HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(
          Hive.get().getConf(), storageHandlerClass);
      sd.getSerdeInfo().setSerializationLib(
          storageHandler.getSerDeClass().getCanonicalName());
    } catch (HiveException e) {
      throw new BreezeException("could not get storagehandler", e);
    }
    sd.getSerdeInfo().getParameters().putAll(serdeParameters);
  }

  public String toHQL() {
    StringBuilder builder = new StringBuilder();
    builder.append(" STORED BY ");
    builder.append(storageHandlerClass);
    int size = serdeParameters.size();
    if (size != 0) {
      builder.append(" WITH SERDEPROPERTIES (");
      int i = 0;
      for (Map.Entry<String, String> entry : serdeParameters.entrySet()) {
        builder.append('"');
        builder.append(entry.getKey());
        builder.append('"');
        builder.append(" = ");
        builder.append('"');
        builder.append(entry.getValue());
        builder.append('"');
        i++;
        if (i != size) {
          builder.append(", ");
        }
      }
    }
    return builder.toString();
  }

  public String toString() {
    return toHQL();
  }

  @Override
  public void addPartition(String storageTableName,
      Map<String, String> partSpec, Hive client, boolean makeLatest)
          throws BreezeException {
    try {
      String hdfsTableName = MetastoreUtil.getHDFSStorageTableFromESStorage(
          storageTableName);
      Table hdfsTable = client.getTable(hdfsTableName);
      // INSERT OVERWRITE TABLE <es-table> PARTITION <partition-cols>
      // SELECT <all-columns> FROM hdfsTable where partitioncols = partition-spec;
      //TODO pass the query to hivedriver
    } catch (HiveException e) {
      throw new BreezeException("Could not add partition", e);
    }
  } */
}
