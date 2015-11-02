package dk.opusmagus.db.dynamo.dac;

import java.util.ArrayList;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class DynamoDBDAC {
	public static void main(String[] args)
	{
		System.out.println("Started DynamoDB Tutorial...");
		
		DynamoDBDAC dac = new DynamoDBDAC();
		
		try 
		{
			dac.createTable("trades");
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Ended DynamoDB Tutorial!");
	}
	
	private AmazonDynamoDBClient client;
	
	public DynamoDBDAC()
	{
		String access_key_id = "my_s3cr3t_k3y";
		String secret_access_key = "my_@acc3s";
		BasicAWSCredentials credentials = new BasicAWSCredentials(access_key_id, secret_access_key);
		//ProfileCredentialsProvider credentials = new ProfileCredentialsProvider();
		client = new AmazonDynamoDBClient(credentials);
		client.setEndpoint("http://localhost:8000");
	}

	private void createTable(String tableName) throws Exception
	{
		DynamoDB dynamoDB = new DynamoDB(client);

		ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));

		ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
		keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));
		        
		CreateTableRequest request = new CreateTableRequest()
				.withTableName(tableName)
				.withKeySchema(keySchema)
				.withAttributeDefinitions(attributeDefinitions)
				.withProvisionedThroughput(new ProvisionedThroughput()
				    .withReadCapacityUnits(5L)
					.withWriteCapacityUnits(6L));

		Table table = dynamoDB.createTable(request);

		table.waitForActive();
	}
}
