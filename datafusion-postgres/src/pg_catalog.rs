use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    as_boolean_array, ArrayRef, BooleanArray, Float64Array, Int16Array, Int32Array, RecordBatch,
    StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{CatalogProviderList, MemTable, SchemaProvider};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{create_udf, SessionContext};

const PG_CATALOG_TABLE_PG_TYPE: &str = "pg_type";
const PG_CATALOG_TABLE_PG_CLASS: &str = "pg_class";
const PG_CATALOG_TABLE_PG_ATTRIBUTE: &str = "pg_attribute";
const PG_CATALOG_TABLE_PG_NAMESPACE: &str = "pg_namespace";
const PG_CATALOG_TABLE_PG_PROC: &str = "pg_proc";
const PG_CATALOG_TABLE_PG_DATABASE: &str = "pg_database";
const PG_CATALOG_TABLE_PG_AM: &str = "pg_am";

pub const PG_CATALOG_TABLES: &[&str] = &[
    PG_CATALOG_TABLE_PG_TYPE,
    PG_CATALOG_TABLE_PG_CLASS,
    PG_CATALOG_TABLE_PG_ATTRIBUTE,
    PG_CATALOG_TABLE_PG_NAMESPACE,
    PG_CATALOG_TABLE_PG_PROC,
    PG_CATALOG_TABLE_PG_DATABASE,
    PG_CATALOG_TABLE_PG_AM,
];

// Create custom schema provider for pg_catalog
#[derive(Debug)]
pub struct PgCatalogSchemaProvider {
    catalog_list: Arc<dyn CatalogProviderList>,
}

#[async_trait]
impl SchemaProvider for PgCatalogSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        PG_CATALOG_TABLES.iter().map(ToString::to_string).collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        match name.to_ascii_lowercase().as_str() {
            PG_CATALOG_TABLE_PG_TYPE => Ok(Some(self.create_pg_type_table())),
            PG_CATALOG_TABLE_PG_AM => Ok(Some(self.create_pg_am_table())),
            PG_CATALOG_TABLE_PG_CLASS => {
                let table = Arc::new(PgClassTable::new(self.catalog_list.clone()));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_NAMESPACE => {
                let table = Arc::new(PgNamespaceTable::new(self.catalog_list.clone()));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_DATABASE => {
                let table = Arc::new(PgDatabaseTable::new(self.catalog_list.clone()));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_ATTRIBUTE => {
                let table = Arc::new(PgAttributeTable::new(self.catalog_list.clone()));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            _ => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        PG_CATALOG_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}

impl PgCatalogSchemaProvider {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> PgCatalogSchemaProvider {
        Self { catalog_list }
    }

    /// Create a mock empty table for pg_type
    fn create_pg_type_table(&self) -> Arc<dyn TableProvider> {
        // Define schema for pg_type
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", DataType::Int32, false),
            Field::new("typlen", DataType::Int16, false),
            // Add other necessary columns
        ]));

        // Create memory table with schema
        let provider = MemTable::try_new(schema, vec![]).unwrap();

        Arc::new(provider)
    }

    /// Create a mock empty table for pg_am
    fn create_pg_am_table(&self) -> Arc<dyn TableProvider> {
        // Define the schema for pg_am
        // This matches PostgreSQL's pg_am table columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("amname", DataType::Utf8, false), // Name of the access method
            Field::new("amhandler", DataType::Int32, false), // OID of handler function
            Field::new("amtype", DataType::Utf8, false), // Type of access method (i=index, t=table)
            Field::new("amstrategies", DataType::Int32, false), // Number of operator strategies
            Field::new("amsupport", DataType::Int32, false), // Number of support routines
            Field::new("amcanorder", DataType::Boolean, false), // Does AM support ordered scans?
            Field::new("amcanorderbyop", DataType::Boolean, false), // Does AM support order by operator result?
            Field::new("amcanbackward", DataType::Boolean, false), // Does AM support backward scanning?
            Field::new("amcanunique", DataType::Boolean, false), // Does AM support unique indexes?
            Field::new("amcanmulticol", DataType::Boolean, false), // Does AM support multi-column indexes?
            Field::new("amoptionalkey", DataType::Boolean, false), // Can first index column be omitted in search?
            Field::new("amsearcharray", DataType::Boolean, false), // Does AM support ScalarArrayOpExpr searches?
            Field::new("amsearchnulls", DataType::Boolean, false), // Does AM support searching for NULL/NOT NULL?
            Field::new("amstorage", DataType::Boolean, false), // Can storage type differ from column type?
            Field::new("amclusterable", DataType::Boolean, false), // Can index be clustered on?
            Field::new("ampredlocks", DataType::Boolean, false), // Does AM manage fine-grained predicate locks?
            Field::new("amcanparallel", DataType::Boolean, false), // Does AM support parallel scan?
            Field::new("amcanbeginscan", DataType::Boolean, false), // Does AM support BRIN index scans?
            Field::new("amcanmarkpos", DataType::Boolean, false), // Does AM support mark/restore positions?
            Field::new("amcanfetch", DataType::Boolean, false), // Does AM support fetching specific tuples?
            Field::new("amkeytype", DataType::Int32, false),    // Type of data in index
        ]));

        // Create memory table with schema
        let provider = MemTable::try_new(schema, vec![]).unwrap();

        Arc::new(provider)
    }
}

#[derive(Debug)]
struct PgClassTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgClassTable {
    fn new(catalog_list: Arc<dyn CatalogProviderList>) -> PgClassTable {
        // Define the schema for pg_class
        // This matches key columns from PostgreSQL's pg_class
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("relname", DataType::Utf8, false), // Name of the table, index, view, etc.
            Field::new("relnamespace", DataType::Int32, false), // OID of the namespace that contains this relation
            Field::new("reltype", DataType::Int32, false), // OID of the data type (composite type) this table describes
            Field::new("reloftype", DataType::Int32, true), // OID of the composite type for typed table, 0 otherwise
            Field::new("relowner", DataType::Int32, false), // Owner of the relation
            Field::new("relam", DataType::Int32, false), // If this is an index, the access method used
            Field::new("relfilenode", DataType::Int32, false), // Name of the on-disk file of this relation
            Field::new("reltablespace", DataType::Int32, false), // Tablespace OID for this relation
            Field::new("relpages", DataType::Int32, false), // Size of the on-disk representation in pages
            Field::new("reltuples", DataType::Float64, false), // Number of tuples
            Field::new("relallvisible", DataType::Int32, false), // Number of all-visible pages
            Field::new("reltoastrelid", DataType::Int32, false), // OID of the TOAST table
            Field::new("relhasindex", DataType::Boolean, false), // True if this is a table and it has (or recently had) any indexes
            Field::new("relisshared", DataType::Boolean, false), // True if this table is shared across all databases
            Field::new("relpersistence", DataType::Utf8, false), // p=permanent table, u=unlogged table, t=temporary table
            Field::new("relkind", DataType::Utf8, false), // r=ordinary table, i=index, S=sequence, v=view, etc.
            Field::new("relnatts", DataType::Int16, false), // Number of user columns
            Field::new("relchecks", DataType::Int16, false), // Number of CHECK constraints
            Field::new("relhasrules", DataType::Boolean, false), // True if table has (or once had) rules
            Field::new("relhastriggers", DataType::Boolean, false), // True if table has (or once had) triggers
            Field::new("relhassubclass", DataType::Boolean, false), // True if table or index has (or once had) any inheritance children
            Field::new("relrowsecurity", DataType::Boolean, false), // True if row security is enabled
            Field::new("relforcerowsecurity", DataType::Boolean, false), // True if row security forced for owners
            Field::new("relispopulated", DataType::Boolean, false), // True if relation is populated (not true for some materialized views)
            Field::new("relreplident", DataType::Utf8, false), // Columns used to form "replica identity" for rows
            Field::new("relispartition", DataType::Boolean, false), // True if table is a partition
            Field::new("relrewrite", DataType::Int32, true), // OID of a rule that rewrites this relation
            Field::new("relfrozenxid", DataType::Int32, false), // All transaction IDs before this have been replaced with a permanent ("frozen") transaction ID
            Field::new("relminmxid", DataType::Int32, false), // All Multixact IDs before this have been replaced with a transaction ID
        ]));

        Self {
            schema,
            catalog_list,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(
        schema: SchemaRef,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Result<RecordBatch> {
        // Vectors to store column data
        let mut oids = Vec::new();
        let mut relnames = Vec::new();
        let mut relnamespaces = Vec::new();
        let mut reltypes = Vec::new();
        let mut reloftypes = Vec::new();
        let mut relowners = Vec::new();
        let mut relams = Vec::new();
        let mut relfilenodes = Vec::new();
        let mut reltablespaces = Vec::new();
        let mut relpages = Vec::new();
        let mut reltuples = Vec::new();
        let mut relallvisibles = Vec::new();
        let mut reltoastrelids = Vec::new();
        let mut relhasindexes = Vec::new();
        let mut relisshareds = Vec::new();
        let mut relpersistences = Vec::new();
        let mut relkinds = Vec::new();
        let mut relnattses = Vec::new();
        let mut relcheckses = Vec::new();
        let mut relhasruleses = Vec::new();
        let mut relhastriggersses = Vec::new();
        let mut relhassubclasses = Vec::new();
        let mut relrowsecurities = Vec::new();
        let mut relforcerowsecurities = Vec::new();
        let mut relispopulateds = Vec::new();
        let mut relreplidents = Vec::new();
        let mut relispartitions = Vec::new();
        let mut relrewrites = Vec::new();
        let mut relfrozenxids = Vec::new();
        let mut relminmxids = Vec::new();

        // Start OID counter (this is simplistic and would need to be more robust in practice)
        let mut next_oid = 10000;

        // Iterate through all catalogs and schemas
        for catalog_name in catalog_list.catalog_names() {
            if let Some(catalog) = catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        let schema_oid = next_oid;
                        next_oid += 1;

                        // Add an entry for the schema itself (as a namespace)
                        // (In a full implementation, this would go in pg_namespace)

                        // Now process all tables in this schema
                        for table_name in schema.table_names() {
                            let table_oid = next_oid;
                            next_oid += 1;

                            if let Some(table) = schema.table(&table_name).await? {
                                // TODO: correct table type
                                let table_type = "r";

                                // Get column count from schema
                                let column_count = table.schema().fields().len() as i16;

                                // Add table entry
                                oids.push(table_oid);
                                relnames.push(table_name.clone());
                                relnamespaces.push(schema_oid);
                                reltypes.push(0); // Simplified: we're not tracking data types
                                reloftypes.push(None);
                                relowners.push(0); // Simplified: no owner tracking
                                relams.push(0); // Default access method
                                relfilenodes.push(table_oid); // Use OID as filenode
                                reltablespaces.push(0); // Default tablespace
                                relpages.push(1); // Default page count
                                reltuples.push(0.0); // No row count stats
                                relallvisibles.push(0);
                                reltoastrelids.push(0);
                                relhasindexes.push(false);
                                relisshareds.push(false);
                                relpersistences.push("p".to_string()); // Permanent
                                relkinds.push(table_type.to_string());
                                relnattses.push(column_count);
                                relcheckses.push(0);
                                relhasruleses.push(false);
                                relhastriggersses.push(false);
                                relhassubclasses.push(false);
                                relrowsecurities.push(false);
                                relforcerowsecurities.push(false);
                                relispopulateds.push(true);
                                relreplidents.push("d".to_string()); // Default
                                relispartitions.push(false);
                                relrewrites.push(None);
                                relfrozenxids.push(0);
                                relminmxids.push(0);
                            }
                        }
                    }
                }
            }
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(relnames)),
            Arc::new(Int32Array::from(relnamespaces)),
            Arc::new(Int32Array::from(reltypes)),
            Arc::new(Int32Array::from_iter(reloftypes.into_iter())),
            Arc::new(Int32Array::from(relowners)),
            Arc::new(Int32Array::from(relams)),
            Arc::new(Int32Array::from(relfilenodes)),
            Arc::new(Int32Array::from(reltablespaces)),
            Arc::new(Int32Array::from(relpages)),
            Arc::new(Float64Array::from_iter(reltuples.into_iter())),
            Arc::new(Int32Array::from(relallvisibles)),
            Arc::new(Int32Array::from(reltoastrelids)),
            Arc::new(BooleanArray::from(relhasindexes)),
            Arc::new(BooleanArray::from(relisshareds)),
            Arc::new(StringArray::from(relpersistences)),
            Arc::new(StringArray::from(relkinds)),
            Arc::new(Int16Array::from(relnattses)),
            Arc::new(Int16Array::from(relcheckses)),
            Arc::new(BooleanArray::from(relhasruleses)),
            Arc::new(BooleanArray::from(relhastriggersses)),
            Arc::new(BooleanArray::from(relhassubclasses)),
            Arc::new(BooleanArray::from(relrowsecurities)),
            Arc::new(BooleanArray::from(relforcerowsecurities)),
            Arc::new(BooleanArray::from(relispopulateds)),
            Arc::new(StringArray::from(relreplidents)),
            Arc::new(BooleanArray::from(relispartitions)),
            Arc::new(Int32Array::from_iter(relrewrites.into_iter())),
            Arc::new(Int32Array::from(relfrozenxids)),
            Arc::new(Int32Array::from(relminmxids)),
        ];

        // Create a record batch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl PartitionStream for PgClassTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let catalog_list = self.catalog_list.clone();
        let schema = Arc::clone(&self.schema);
        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async move { Self::get_data(schema, catalog_list).await }),
        ))
    }
}

#[derive(Debug)]
struct PgNamespaceTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgNamespaceTable {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
        // Define the schema for pg_namespace
        // This matches the columns from PostgreSQL's pg_namespace
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("nspname", DataType::Utf8, false), // Name of the namespace (schema)
            Field::new("nspowner", DataType::Int32, false), // Owner of the namespace
            Field::new("nspacl", DataType::Utf8, true), // Access privileges
            Field::new("options", DataType::Utf8, true), // Schema-level options
        ]));

        Self {
            schema,
            catalog_list,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(
        schema: SchemaRef,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Result<RecordBatch> {
        // Vectors to store column data
        let mut oids = Vec::new();
        let mut nspnames = Vec::new();
        let mut nspowners = Vec::new();
        let mut nspacls: Vec<Option<String>> = Vec::new();
        let mut options: Vec<Option<String>> = Vec::new();

        // Start OID counter (should be consistent with the values used in pg_class)
        let mut next_oid = 10000;

        // Add standard PostgreSQL system schemas
        // pg_catalog schema (OID 11)
        oids.push(11);
        nspnames.push("pg_catalog".to_string());
        nspowners.push(10); // Default superuser
        nspacls.push(None);
        options.push(None);

        // public schema (OID 2200)
        oids.push(2200);
        nspnames.push("public".to_string());
        nspowners.push(10); // Default superuser
        nspacls.push(None);
        options.push(None);

        // information_schema (OID 12)
        oids.push(12);
        nspnames.push("information_schema".to_string());
        nspowners.push(10); // Default superuser
        nspacls.push(None);
        options.push(None);

        // Now add all schemas from DataFusion catalogs
        for catalog_name in catalog_list.catalog_names() {
            if let Some(catalog) = catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    // Skip schemas we've already added as system schemas
                    if schema_name == "pg_catalog"
                        || schema_name == "public"
                        || schema_name == "information_schema"
                    {
                        continue;
                    }

                    let schema_oid = next_oid;
                    next_oid += 1;

                    oids.push(schema_oid);
                    nspnames.push(schema_name.clone());
                    nspowners.push(10); // Default owner
                    nspacls.push(None);
                    options.push(None);
                }
            }
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(nspnames)),
            Arc::new(Int32Array::from(nspowners)),
            Arc::new(StringArray::from_iter(nspacls.into_iter())),
            Arc::new(StringArray::from_iter(options.into_iter())),
        ];

        // Create a full record batch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl PartitionStream for PgNamespaceTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let catalog_list = self.catalog_list.clone();
        let schema = Arc::clone(&self.schema);
        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async move { Self::get_data(schema, catalog_list).await }),
        ))
    }
}

#[derive(Debug)]
struct PgDatabaseTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgDatabaseTable {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
        // Define the schema for pg_database
        // This matches PostgreSQL's pg_database table columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("datname", DataType::Utf8, false), // Database name
            Field::new("datdba", DataType::Int32, false), // Database owner's user ID
            Field::new("encoding", DataType::Int32, false), // Character encoding
            Field::new("datcollate", DataType::Utf8, false), // LC_COLLATE for this database
            Field::new("datctype", DataType::Utf8, false), // LC_CTYPE for this database
            Field::new("datistemplate", DataType::Boolean, false), // If true, database can be used as a template
            Field::new("datallowconn", DataType::Boolean, false), // If false, no one can connect to this database
            Field::new("datconnlimit", DataType::Int32, false), // Max number of concurrent connections (-1=no limit)
            Field::new("datlastsysoid", DataType::Int32, false), // Last system OID in database
            Field::new("datfrozenxid", DataType::Int32, false), // Frozen XID for this database
            Field::new("datminmxid", DataType::Int32, false),   // Minimum multixact ID
            Field::new("dattablespace", DataType::Int32, false), // Default tablespace for this database
            Field::new("datacl", DataType::Utf8, true),          // Access privileges
        ]));

        Self {
            schema,
            catalog_list,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(
        schema: SchemaRef,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Result<RecordBatch> {
        // Vectors to store column data
        let mut oids = Vec::new();
        let mut datnames = Vec::new();
        let mut datdbas = Vec::new();
        let mut encodings = Vec::new();
        let mut datcollates = Vec::new();
        let mut datctypes = Vec::new();
        let mut datistemplates = Vec::new();
        let mut datallowconns = Vec::new();
        let mut datconnlimits = Vec::new();
        let mut datlastsysoids = Vec::new();
        let mut datfrozenxids = Vec::new();
        let mut datminmxids = Vec::new();
        let mut dattablespaces = Vec::new();
        let mut datacles: Vec<Option<String>> = Vec::new();

        // Start OID counter (this is simplistic and would need to be more robust in practice)
        let mut next_oid = 16384; // Standard PostgreSQL starting OID for user databases

        // Add a record for each catalog (treating catalogs as "databases")
        for catalog_name in catalog_list.catalog_names() {
            let oid = next_oid;
            next_oid += 1;

            oids.push(oid);
            datnames.push(catalog_name.clone());
            datdbas.push(10); // Default owner (assuming 10 = postgres user)
            encodings.push(6); // 6 = UTF8 in PostgreSQL
            datcollates.push("en_US.UTF-8".to_string()); // Default collation
            datctypes.push("en_US.UTF-8".to_string()); // Default ctype
            datistemplates.push(false);
            datallowconns.push(true);
            datconnlimits.push(-1); // No connection limit
            datlastsysoids.push(100000); // Arbitrary last system OID
            datfrozenxids.push(1); // Simplified transaction ID
            datminmxids.push(1); // Simplified multixact ID
            dattablespaces.push(1663); // Default tablespace (1663 = pg_default in PostgreSQL)
            datacles.push(None); // No specific ACLs
        }

        // Always include a "postgres" database entry if not already present
        // (This is for compatibility with tools that expect it)
        if !datnames.contains(&"postgres".to_string()) {
            let oid = next_oid;

            oids.push(oid);
            datnames.push("postgres".to_string());
            datdbas.push(10);
            encodings.push(6);
            datcollates.push("en_US.UTF-8".to_string());
            datctypes.push("en_US.UTF-8".to_string());
            datistemplates.push(false);
            datallowconns.push(true);
            datconnlimits.push(-1);
            datlastsysoids.push(100000);
            datfrozenxids.push(1);
            datminmxids.push(1);
            dattablespaces.push(1663);
            datacles.push(None);
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(datnames)),
            Arc::new(Int32Array::from(datdbas)),
            Arc::new(Int32Array::from(encodings)),
            Arc::new(StringArray::from(datcollates)),
            Arc::new(StringArray::from(datctypes)),
            Arc::new(BooleanArray::from(datistemplates)),
            Arc::new(BooleanArray::from(datallowconns)),
            Arc::new(Int32Array::from(datconnlimits)),
            Arc::new(Int32Array::from(datlastsysoids)),
            Arc::new(Int32Array::from(datfrozenxids)),
            Arc::new(Int32Array::from(datminmxids)),
            Arc::new(Int32Array::from(dattablespaces)),
            Arc::new(StringArray::from_iter(datacles.into_iter())),
        ];

        // Create a full record batch
        let full_batch = RecordBatch::try_new(schema.clone(), arrays)?;
        Ok(full_batch)
    }
}

impl PartitionStream for PgDatabaseTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let catalog_list = self.catalog_list.clone();
        let schema = Arc::clone(&self.schema);
        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async move { Self::get_data(schema, catalog_list).await }),
        ))
    }
}

pub fn create_current_schemas_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let input = as_boolean_array(&args[0]);

        // Create a UTF8 array with a single value
        let mut values = vec!["public"];
        // include implicit schemas
        if input.value(0) {
            values.push("information_schema");
            values.push("pg_catalog");
        }

        let list_array = SingleRowListArrayBuilder::new(Arc::new(StringArray::from(values)));

        let array: ArrayRef = Arc::new(list_array.build_list_array());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_schemas",
        vec![DataType::Boolean],
        DataType::List(Arc::new(Field::new("schema", DataType::Utf8, false))),
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_current_schema_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Create a UTF8 array with a single value
        let mut builder = StringBuilder::new();
        builder.append_value("public");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_schema",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

/// Install pg_catalog and postgres UDFs to current `SessionContext`
pub fn setup_pg_catalog(
    session_context: &SessionContext,
    catalog_name: &str,
) -> Result<(), Box<DataFusionError>> {
    let pg_catalog = PgCatalogSchemaProvider::new(session_context.state().catalog_list().clone());
    session_context
        .catalog(catalog_name)
        .ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Catalog not found when registering pg_catalog: {}",
                catalog_name
            ))
        })?
        .register_schema("pg_catalog", Arc::new(pg_catalog))?;

    session_context.register_udf(create_current_schema_udf());
    session_context.register_udf(create_current_schemas_udf());

    Ok(())
}

#[derive(Debug)]
struct PgAttributeTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgAttributeTable {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
        // Define the schema for pg_attribute
        // This matches key columns from PostgreSQL's pg_attribute
        let schema = Arc::new(Schema::new(vec![
            Field::new("attrelid", DataType::Int32, false), // OID of the table this column belongs to
            Field::new("attname", DataType::Utf8, false),   // Column name
            Field::new("atttypid", DataType::Int32, false), // Data type OID
            Field::new("attlen", DataType::Int16, false),   // Length of the type
            Field::new("attnum", DataType::Int16, false), // Column number (1-based for user columns)
            Field::new("attcacheoff", DataType::Int32, false), // Always -1 in storage
            Field::new("atttypmod", DataType::Int32, false), // Type modifier (-1 if not applicable)
            Field::new("attndims", DataType::Int16, false), // Number of array dimensions (0 if not array)
            Field::new("attbyval", DataType::Boolean, false), // True if type is passed by value
            Field::new("attalign", DataType::Utf8, false),  // Type alignment requirement
            Field::new("attstorage", DataType::Utf8, false), // Storage strategy
            Field::new("attcompression", DataType::Utf8, false), // Compression method
            Field::new("attnotnull", DataType::Boolean, false), // True if column has NOT NULL constraint
            Field::new("atthasdef", DataType::Boolean, false),  // True if column has default value
            Field::new("atthasmissing", DataType::Boolean, false), // True if column has missing value
            Field::new("attidentity", DataType::Utf8, false),      // Identity column type
            Field::new("attgenerated", DataType::Utf8, false),     // Generated column type
            Field::new("attisdropped", DataType::Boolean, false),  // True if column is dropped
            Field::new("attislocal", DataType::Boolean, false), // True if column is defined locally
            Field::new("attinhcount", DataType::Int16, false),  // Number of direct ancestors
            Field::new("attcollation", DataType::Int32, false), // Collation OID
            Field::new("attacl", DataType::Utf8, true),         // Access privileges
            Field::new("attoptions", DataType::Utf8, true),     // Attribute-level options
            Field::new("attfdwoptions", DataType::Utf8, true),  // Foreign data wrapper options
            Field::new("attmissingval", DataType::Utf8, true),  // Missing value for column
        ]));

        Self {
            schema,
            catalog_list,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(
        schema: SchemaRef,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Result<RecordBatch> {
        use crate::datatypes::into_pg_type;

        // Vectors to store column data
        let mut attrelids = Vec::new();
        let mut attnames = Vec::new();
        let mut atttypids = Vec::new();
        let mut attlens = Vec::new();
        let mut attnums = Vec::new();
        let mut attcacheoffs = Vec::new();
        let mut atttypemods = Vec::new();
        let mut attndimss = Vec::new();
        let mut attbyvals = Vec::new();
        let mut attaligns = Vec::new();
        let mut attstorages = Vec::new();
        let mut attcompressions = Vec::new();
        let mut attnotnulls = Vec::new();
        let mut atthasdefs = Vec::new();
        let mut atthasmissings = Vec::new();
        let mut attidentities = Vec::new();
        let mut attgenerateds = Vec::new();
        let mut attisdroppeds = Vec::new();
        let mut attislocals = Vec::new();
        let mut attinhcounts = Vec::new();
        let mut attcollations = Vec::new();
        let mut attacls: Vec<Option<String>> = Vec::new();
        let mut attoptions: Vec<Option<String>> = Vec::new();
        let mut attfdwoptions: Vec<Option<String>> = Vec::new();
        let mut attmissingvals: Vec<Option<String>> = Vec::new();

        // Start OID counter (should be consistent with pg_class)
        let mut next_oid = 10000;

        // Iterate through all catalogs and schemas
        for catalog_name in catalog_list.catalog_names() {
            if let Some(catalog) = catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema_provider) = catalog.schema(&schema_name) {
                        let _schema_oid = next_oid;
                        next_oid += 1;

                        // Process all tables in this schema
                        for table_name in schema_provider.table_names() {
                            let table_oid = next_oid;
                            next_oid += 1;

                            if let Some(table) = schema_provider.table(&table_name).await? {
                                let table_schema = table.schema();

                                // Process each column in the table
                                for (column_idx, field) in table_schema.fields().iter().enumerate()
                                {
                                    let pg_type = into_pg_type(field.data_type())
                                        .unwrap_or_else(|_| pgwire::api::Type::UNKNOWN);

                                    attrelids.push(table_oid);
                                    attnames.push(field.name().clone());
                                    atttypids.push(pg_type.oid() as i32);

                                    // Set attlen based on data type
                                    let type_len = match field.data_type() {
                                        DataType::Int8 => 1,
                                        DataType::Int16 => 2,
                                        DataType::Int32 => 4,
                                        DataType::Int64 => 8,
                                        DataType::Float32 => 4,
                                        DataType::Float64 => 8,
                                        DataType::Boolean => 1,
                                        DataType::Date32 => 4,
                                        DataType::Date64 => 8,
                                        DataType::Timestamp(_, _) => 8,
                                        DataType::Utf8 | DataType::LargeUtf8 => -1, // Variable length
                                        _ => -1, // Variable length for other types
                                    };
                                    attlens.push(type_len);

                                    attnums.push((column_idx + 1) as i16); // 1-based column numbers
                                    attcacheoffs.push(-1); // Always -1 in storage
                                    atttypemods.push(-1); // No type modifier by default

                                    // Check if it's an array type
                                    let is_array = matches!(
                                        field.data_type(),
                                        DataType::List(_)
                                            | DataType::LargeList(_)
                                            | DataType::FixedSizeList(_, _)
                                    );
                                    attndimss.push(if is_array { 1 } else { 0 });

                                    // Set attbyval based on type
                                    let by_val = matches!(
                                        field.data_type(),
                                        DataType::Boolean
                                            | DataType::Int8
                                            | DataType::Int16
                                            | DataType::Int32
                                            | DataType::Float32
                                            | DataType::Date32
                                    );
                                    attbyvals.push(by_val);

                                    // Set alignment based on type size
                                    let alignment = match field.data_type() {
                                        DataType::Int8 | DataType::Boolean => "c",
                                        DataType::Int16 => "s",
                                        DataType::Int32 | DataType::Float32 | DataType::Date32 => {
                                            "i"
                                        }
                                        DataType::Int64
                                        | DataType::Float64
                                        | DataType::Date64
                                        | DataType::Timestamp(_, _) => "d",
                                        _ => "i", // Default to integer alignment
                                    };
                                    attaligns.push(alignment.to_string());

                                    // Storage strategy - 'p' for plain, 'x' for extended
                                    let storage = match field.data_type() {
                                        DataType::Utf8
                                        | DataType::LargeUtf8
                                        | DataType::Binary
                                        | DataType::LargeBinary => "x",
                                        _ => "p",
                                    };
                                    attstorages.push(storage.to_string());

                                    attcompressions.push("".to_string()); // No compression by default
                                    attnotnulls.push(!field.is_nullable()); // NOT NULL constraint
                                    atthasdefs.push(false); // No default values implemented yet
                                    atthasmissings.push(false); // No missing values
                                    attidentities.push("".to_string()); // No identity columns
                                    attgenerateds.push("".to_string()); // No generated columns
                                    attisdroppeds.push(false); // Column is not dropped
                                    attislocals.push(true); // Column is defined locally
                                    attinhcounts.push(0); // No inheritance
                                    attcollations.push(0); // No specific collation
                                    attacls.push(None); // No specific ACLs
                                    attoptions.push(None); // No options
                                    attfdwoptions.push(None); // No FDW options
                                    attmissingvals.push(None); // No missing values
                                }
                            }
                        }
                    }
                }
            }
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(attrelids)),
            Arc::new(StringArray::from(attnames)),
            Arc::new(Int32Array::from(atttypids)),
            Arc::new(Int16Array::from(attlens)),
            Arc::new(Int16Array::from(attnums)),
            Arc::new(Int32Array::from(attcacheoffs)),
            Arc::new(Int32Array::from(atttypemods)),
            Arc::new(Int16Array::from(attndimss)),
            Arc::new(BooleanArray::from(attbyvals)),
            Arc::new(StringArray::from(attaligns)),
            Arc::new(StringArray::from(attstorages)),
            Arc::new(StringArray::from(attcompressions)),
            Arc::new(BooleanArray::from(attnotnulls)),
            Arc::new(BooleanArray::from(atthasdefs)),
            Arc::new(BooleanArray::from(atthasmissings)),
            Arc::new(StringArray::from(attidentities)),
            Arc::new(StringArray::from(attgenerateds)),
            Arc::new(BooleanArray::from(attisdroppeds)),
            Arc::new(BooleanArray::from(attislocals)),
            Arc::new(Int16Array::from(attinhcounts)),
            Arc::new(Int32Array::from(attcollations)),
            Arc::new(StringArray::from_iter(attacls.into_iter())),
            Arc::new(StringArray::from_iter(attoptions.into_iter())),
            Arc::new(StringArray::from_iter(attfdwoptions.into_iter())),
            Arc::new(StringArray::from_iter(attmissingvals.into_iter())),
        ];

        // Create a record batch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl PartitionStream for PgAttributeTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let catalog_list = self.catalog_list.clone();
        let schema = Arc::clone(&self.schema);
        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async move { Self::get_data(schema, catalog_list).await }),
        ))
    }
}
