import com.snowflake.kafka.model.StreamRecord;

public class test_json {
    public static void main(String[] args) {
        StreamRecord record = new StreamRecord("TEST_STREAM", "INSERT");
        record.setRowId("test-row-123");
        record.setStreamOffset(1000L);
        record.addColumn("customer_id", 12345);
        record.addColumn("customer_name", "John Doe");
        record.addColumn("email", "john.doe@example.com");
        record.addMetadata("source", "integration-test");
        
        String json = record.toJson();
        System.out.println("Generated JSON:");
        System.out.println(json);
    }
}
