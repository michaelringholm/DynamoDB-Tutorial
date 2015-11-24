package dk.opusmagus.db.dynamo.dac;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

public class DynamoDBDAC {
	private DynamoDB dynamoDB;
	private AmazonDynamoDBClient client;
	
	public DynamoDBDAC()
	{
		String access_key_id = "my_s3cr3t_k3y";
		String secret_access_key = "my_@acc3s";
		BasicAWSCredentials credentials = new BasicAWSCredentials(access_key_id, secret_access_key);
		//ProfileCredentialsProvider credentials = new ProfileCredentialsProvider();
		client = new AmazonDynamoDBClient(credentials);
		client.setEndpoint("http://localhost:8000");
		dynamoDB = new DynamoDB(client);
		initTableMap();
	}
	
	public static void main(String[] args)
	{
		System.out.println("Started DynamoDB Tutorial...");
		
		DynamoDBDAC dac = new DynamoDBDAC();
		
		try 
		{			
			String tableName = "trades";
			//Table trades = dac.getTable(tableName);						
			
			//if(trades == null)
			
			dac.dropTable(tableName);
			dac.createTable(tableName);
			dac.generateMassData(tableName);
			List<Map<String, AttributeValue>> data = dac.getData(tableName);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Ended DynamoDB Tutorial!");
	}
	
	private void generateMassData(String tableName) {
		for(int i=0; i<0; i++)
		{
			new Thread() {
				public void run() {
					insertData(getTable("trades"));					
				}
			}.start();;			
		}
		
		insertData(getTable("trades"));
	}

	private List<Map<String, AttributeValue>> getData(String tableName) {
		//ItemCollection<QueryOutcome> result = trades.query("Id", 1);
		//result.getTotalCount();
		
		//ItemCollection<ScanOutcome> scanOutcome = trades.scan(ScanFilter);
		
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		//expressionAttributeValues.put(":val", new AttributeValue().withN("100"));
		expressionAttributeValues.put(":tradeType", new AttributeValue().withS("FX Swap"));
			
		ScanRequest scanRequest = new ScanRequest()
	    .withTableName(tableName)
	    .withFilterExpression("TradeType = :tradeType")
	    //.withFilterExpression("Id < :val")
	    //.withProjectionExpression("Id")
	    //.withProjectionExpression("Id")
	    .withExpressionAttributeValues(expressionAttributeValues);
		
		ScanResult scanResult = client.scan(scanRequest);
		for (Map<String, AttributeValue> item : scanResult.getItems()) {
		    System.out.println(item);
		}
		
		return scanResult.getItems();
	}

	private void insertData(Table trades) {
		Item item;
		//item = new Item();

		String[] tradeTypes = new String[7];
		tradeTypes[0] = "FX Spot";
		tradeTypes[1] = "FX Option";
		tradeTypes[2] = "FX Swap";
		tradeTypes[3] = "Cap";
		tradeTypes[4] = "Floor";
		tradeTypes[5] = "Swap";
		tradeTypes[6] = "Swaption";
		
		Random rand = new Random();		
		
		TableWriteItems tableWriteItems = new TableWriteItems("trades");
		List<Item> itemsToPut = new ArrayList<Item>();
		//item.withPrimaryKey("Id", "1234");
		//item.withPrimaryKey("Id", KeyType.fromValue("1234"));
		for(int i=0; i<500; i++)
		{
			PrimaryKey pk = new PrimaryKey("Id", i);
			item = new Item();
			item.withPrimaryKey(pk);
			int tradeTypeIndex = rand.nextInt(tradeTypes.length);
			item.withString("TradeType", tradeTypes[tradeTypeIndex]);
			//new PutItemRequest()
			
			
			//trades.putItem(item);
			itemsToPut.add(item);
			
			
			if(i%20 == 0)
			{				
				System.out.println(i + " documents insertd so far");
				tableWriteItems.withItemsToPut(itemsToPut);
				dynamoDB.batchWriteItem(tableWriteItems);
				itemsToPut.clear();				
			}			
		}
		
	}

	private Table getTable(String tableName) {
		return dynamoDB.getTable(tableName);
	}	
	
	private void dropTable(String tableName)
	{
		try
		{
			if(tableMap.containsKey(tableName))
			{
				Table table = dynamoDB.getTable(tableName);			
	
				table.delete();
				table.waitForDelete();
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}		
	}
	
	private Map<String, Table> tableMap = new HashMap<String, Table>();
	private TableCollection<ListTablesResult> initTableMap()
	{
		TableCollection<ListTablesResult> tables = dynamoDB.listTables();
		Iterator<Table> iterator = tables.iterator();

		while (iterator.hasNext()) {
			Table table = iterator.next();
			System.out.println(table.getTableName());
			tableMap.put(table.getTableName(), table);
		}
		
		return tables;
	}

	private void createTable(String tableName) throws Exception
	{		

		ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType(ScalarAttributeType.N));

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
