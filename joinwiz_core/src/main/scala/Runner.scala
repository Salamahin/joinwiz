import org.apache.spark.sql.SparkSession

object Runner extends App{
  val ss = SparkSession.builder().master("local")
    .getOrCreate()

  import org.apache.spark.sql.types._

  val schema = StructType(Seq(
    StructField("__operation_type", StringType, true),
    StructField("__sequence_number", LongType, true),
    StructField("__timestamp", TimestampType, true),
    StructField("Equipment", StringType, true),
    StructField("ValidityEndDate", StringType, true),
    StructField("EquipUsagePeriodSequenceNumber", StringType, true),
    StructField("ABCIndicator", StringType, true),
    StructField("AcquisitionValue", DecimalType(38, 6), true),
    StructField("AddressID", StringType, true),
    StructField("MasterFixedAsset", StringType, true),
    StructField("CompanyCode", StringType, true),
    StructField("CostCenter", StringType, true),
    StructField("ControllingArea", StringType, true),
    StructField("Currency", StringType, true),
    StructField("OperationStartDate", StringType, true),
    StructField("TechnicalObjectType", StringType, true),
    StructField("FunctionalLocation", StringType, true),
    StructField("InventoryNumber", StringType, true),
    StructField("AssetLocation", StringType, true),
    StructField("MaintenancePlant", StringType, true),
    StructField("AssetRoom", StringType, true),
    StructField("Material", StringType, true),
    StructField("MaintenancePlanningPlant", StringType, true),
    StructField("PlantSection", StringType, true),
    StructField("MaintenancePlannerGroup", StringType, true),
    StructField("WBSElement", StringType, true),
    StructField("EquipmentCategory", StringType, true),
    StructField("CreationDate", StringType, true),
    StructField("Plant", StringType, true),
    StructField("MainWorkCenterInternalID", StringType, true),
    StructField("MainWorkCenter", StringType, true),
    StructField("MainWorkCenterPlant", StringType, true),
    StructField("SettlementOrder", StringType, true),
    StructField("WorkCenterInternalID", StringType, true),
    StructField("WorkCenter", StringType, true),
    StructField("WorkCenterPlant", StringType, true),
    StructField("LastChangeDateTime", TimestampType, true),
    StructField("EquipmentIsMarkedForDeletion", StringType, true),
    StructField("NextEquipUsagePeriodSqncNmbr", StringType, true),
    StructField("MaintObjectLocAcctAssgmtNmbr", StringType, true),
    StructField("WorkCenterTypeCode", StringType, true),
    StructField("CatalogProfile", StringType, true),
    StructField("SuperordinateEquipment", StringType, true),
    StructField("TechnicalObjectSortCode", StringType, true),
    StructField("ConstructionMaterial", StringType, true),
    StructField("ValidityStartDate", StringType, true),
    StructField("ValidityEndTime", StringType, true),
    StructField("ManufacturerPartNmbr", StringType, true),
    StructField("Division", StringType, true),
    StructField("StorageLocation", StringType, true),
    StructField("AssetManufacturerName", StringType, true),
    StructField("ManufacturerPartTypeName", StringType, true),
    StructField("ManufacturerCountry", StringType, true),
    StructField("ConstructionYear", StringType, true),
    StructField("ConstructionMonth", StringType, true),
    StructField("AcquisitionDate", StringType, true),
    StructField("MaintObjectInternalID", StringType, true),
    StructField("SerialNumber", StringType, true),
    StructField("Customer", StringType, true),
    StructField("AuthorizationGroup", StringType, true),
    StructField("GrossWeight", DecimalType(13, 3), true),
    StructField("GrossWeightUnit", StringType, true),
    StructField("SizeOrDimensionText", StringType, true),
    StructField("Batch", StringType, true),
    StructField("Supplier", StringType, true),
    StructField("EquipmentEndOfUseDate", StringType, true),
    StructField("MaintObjectFreeDefinedAttrib", StringType, true),
    StructField("BusinessArea", StringType, true),
    StructField("WBSElementInternalID", StringType, true),
    StructField("FixedAsset", StringType, true),
    StructField("LinearDataStartPoint", StringType, true),
    StructField("LinearDataEndPoint", StringType, true),
    StructField("LinearDataLength", DecimalType(31, 14), true),
    StructField("LinearDataUnitOfMeasure", StringType, true)
  ))


  ss.read
    .schema(schema)
    .parquet("/Users/salamahin/part-QCHLtLi66Zw1j2C8-aT_mnj72xeX4pFwR.parquet")
    .repartition(1)
    .drop("LinearDataUnitOfMeasure")
    .limit(100)
    .write
    .option("header", "true")
    .csv("output")
}
