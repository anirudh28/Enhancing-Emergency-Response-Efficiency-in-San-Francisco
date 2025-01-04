import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.naming.Context;

public class FireIncidentTimeMapper extends Mapper<Object, Text, Text, Text> {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private static final int ALARM_DTTM_INDEX = 6;
    private static final int ARRIVAL_DTTM_INDEX = 7;
    private static final int CLOSE_DTTM_INDEX = 8;
    private static final int EXPECTED_FIELD_COUNT = 30;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        
        // Skip header rows and rows with missing fields
        if (fields[0].equals("Incident Number") || fields.length != EXPECTED_FIELD_COUNT) {
            return;
        }

        if(fields[ALARM_DTTM_INDEX].isEmpty() || fields[ARRIVAL_DTTM_INDEX].isEmpty() || fields[CLOSE_DTTM_INDEX].isEmpty()) {
            return;
        }

        try {
            Date alarmDtTm = dateFormat.parse(fields[ALARM_DTTM_INDEX]);
            Date arrivalDtTm = dateFormat.parse(fields[ARRIVAL_DTTM_INDEX]);
            Date closeDtTm = dateFormat.parse(fields[CLOSE_DTTM_INDEX]);

            // Check chronological order of timestamps and also remove with missing timestamps
            if (alarmDtTm.after(arrivalDtTm) || alarmDtTm.after(closeDtTm) || arrivalDtTm.after(closeDtTm)) {
                // Remove rows with timestamps in the wrong order
                return;
            }

            long responseTime = arrivalDtTm.getTime() - alarmDtTm.getTime(); // in milliseconds
            long turnaroundTime = closeDtTm.getTime() - alarmDtTm.getTime(); // in milliseconds

            context.write(new Text("Response Time"), new Text(String.valueOf(responseTime)));
            context.write(new Text("Turnaround Time"), new Text(String.valueOf(turnaroundTime)));

        } catch (ParseException e) {
            System.err.println("Error parsing date: " + e.getMessage());
        }
    }
}