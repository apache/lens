import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class SampleUdf extends UDF {
    public Text evaluate(final Text s, Text sleepTime) throws InterruptedException {

        if(sleepTime!=null){
            Thread.sleep(Long.parseLong(sleepTime.toString()));
        }else{
            Thread.sleep(180000);
        }

        if (s == null) { return null; }

        return new Text(s.toString().toLowerCase());
    }
}