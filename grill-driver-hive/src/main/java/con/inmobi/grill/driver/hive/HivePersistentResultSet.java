package con.inmobi.grill.driver.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TableSchema;
import com.inmobi.grill.api.GrillResultSetMetadata;
import com.inmobi.grill.api.PersistentResultSet;

public class HivePersistentResultSet implements PersistentResultSet {
	private final Path path;
	private final TableSchema metadata;
	
  public HivePersistentResultSet(Path resultSetPath, TableSchema hiveResultSetMetadata) {
  	this.path = resultSetPath;
  	this.metadata = hiveResultSetMetadata;
	}

	@Override
  public int size() {
  	return metadata.getSize();
  }

  @Override
  public String getOutputPath() {
  	return path.toString();
  }

  @Override
  public GrillResultSetMetadata getMetadata() {
  	final List<ColumnDescriptor> descriptors = metadata.getColumnDescriptors();
  	if (descriptors == null) {
  		return null;
  	}
  	return new GrillResultSetMetadata() {
			@Override
			public List<Column> getColumns() {
				List<Column> columns = new ArrayList<Column>(descriptors.size());
				for (ColumnDescriptor colDesc : descriptors) {
					columns.add(new Column(colDesc.getName(), colDesc.getTypeName()));
				}
				return columns;
			}
  	};
  }
}
