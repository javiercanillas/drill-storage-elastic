 
package org.apache.drill.exec.store.elasticsearch.schema;

import java.util.ArrayList;
import java.util.Set;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.elasticsearch.ElasticSearchScanSpec;

import com.google.common.primitives.Bytes;

public class DrillElasticsearchTable extends DynamicDrillTable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillElasticsearchTable.class);


  public DrillElasticsearchTable(StoragePlugin plugin, String storageEngineName, String userName, ElasticSearchScanSpec scanSpec) {
    super(plugin, storageEngineName, scanSpec);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {

    return super.getRowType(typeFactory);
  }
  
  
//  @Override
//  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//    ArrayList<RelDataType> typeList = new ArrayList<>();
//    ArrayList<String> fieldNameList = new ArrayList<>();
//    
//    
//    // 拿到相关的字段
//    fieldNameList.add(ROW_KEY);
//    typeList.add(typeFactory.createSqlType(SqlTypeName.ANY));
//
//    Set<byte[]> families = tableDesc.getFamiliesKeys();
//    for (byte[] family : families) {
//      fieldNameList.add(Bytes.toString(family));
//      // 列族 key value
//      typeList.add(typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.ANY)));
//    }
//    return typeFactory.createStructType(typeList, fieldNameList);
//  }
 
}
