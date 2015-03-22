package partitionprune;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * A Hive udf that runs at query compile time and can be used to filter partitions based on the partitions
 * contained in a different(or the same) table.
 */
@UDFType(deterministic=true)
public class PartitionPrune extends UDF {
	
	private Set<String> partitionValues;
	
	private String fullyQualifiedTableName;
	private String filterPartitionColumn;
	private String filterPartitionValue;
	private String partitionColumn;
	
	
	/**
	 * Usage:
	 * partition_prune('db.source_table','batch_id','2','date', tbl.date)
	 * 
	 * if db.source_table contains the partitions
	 *   batch_id=1, date=2015-03-10
	 *   batch_id=1, date=2015-03-11
	 *   batch_id=2, date=2015-03-11
	 *   batch_id=2, date=2015-03-12
	 * then the function will return true for when tbl.date is 2015-03-11 or 2015-03-12
	 */
	public boolean evaluate(
			String fullyQualifiedTableName,
			String filterPartitionColumn,
			String filterPartitionValue,
			String partitionColumn,
			String partitionValue) throws HiveException{
		try {
			if (fullyQualifiedTableName.equals(this.fullyQualifiedTableName) &&
					filterPartitionColumn.equals(this.filterPartitionColumn) &&
					filterPartitionValue.equals(this.filterPartitionValue) &&
					partitionColumn.equals(this.partitionColumn)){
				return partitionValues.contains(partitionValue.toLowerCase());
				
			}else {
				this.fullyQualifiedTableName = fullyQualifiedTableName;
				this.filterPartitionColumn = filterPartitionColumn;
				this.filterPartitionValue = filterPartitionValue;
				this.partitionColumn = partitionColumn;
				
				String[] tableNameComponents = fullyQualifiedTableName.split("\\.");
				String dbName = tableNameComponents[0];
				String tableName = tableNameComponents[1];
				String filter = filterPartitionColumn + " = \"" + filterPartitionValue + "\"";
				HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
				int fieldIndex = -1;
				List<FieldSchema> partitionKeys = hiveMetaStoreClient.getTable(dbName, tableName).getPartitionKeys();
				for (int i=0; i<partitionKeys.size(); i++){
					FieldSchema fieldSchema = partitionKeys.get(i);
					if (fieldSchema.getName().toLowerCase().equals(partitionColumn.toLowerCase())){
						fieldIndex = i;
						break;
					}
				}
				if (fieldIndex == -1){
					throw new HiveException("Partition column " + partitionColumn + " not found");
				}
				
				
				List<Partition> matchingPartitions = hiveMetaStoreClient.listPartitionsByFilter(dbName, tableName, filter, (short) -1);
				partitionValues = new HashSet<String>();
				for (Partition partition : matchingPartitions){
					partitionValues.add(partition.getValues().get(fieldIndex).toLowerCase());
				}
				return partitionValues.contains(partitionValue.toLowerCase());
				
			}
		} catch (HiveException e){
			throw e;
		} catch (Exception e){
			throw new HiveException("Something went wrong!", e);
		}
		
		
	}
}
