package con.inmobi.grill.driver.hive;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import com.inmobi.grill.api.GrillResultSetMetadata;
import com.inmobi.grill.api.PersistentResultSet;

public class HiveResultSet implements PersistentResultSet {

  public HiveResultSet(ASTNode destTree, CommandProcessorResponse response) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getOutputPath() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GrillResultSetMetadata getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

}
