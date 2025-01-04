import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import javax.naming.Context;

import java.io.IOException;

public class FireIncidentsInjuredDeathMapper
        extends Mapper<Object, Text, Text, Text> {

    private static final int FIRE_FATALITIES_INDEX = 16;
    private static final int FIRE_INJURIES_INDEX = 17;
    private static final int CIVILIAN_FATALITIES_INDEX = 18;
    private static final int CIVILIAN_INJURIES_INDEX = 19;

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        
        // Skip header row
        if (fields[0].equals("Incident Number")) {
            return;
        }

        String[] columns = {"Fire Fatalities", "Fire Injuries", "Civilian Fatalities", "Civilian Injuries"};
        int[] indices = {FIRE_FATALITIES_INDEX, FIRE_INJURIES_INDEX, CIVILIAN_FATALITIES_INDEX, CIVILIAN_INJURIES_INDEX};

        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i];
            String columnValue = fields[indices[i]];
            context.write(new Text(columnName), new Text(columnValue));
        }
    }
}