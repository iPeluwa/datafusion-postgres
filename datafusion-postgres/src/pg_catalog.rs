use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    as_boolean_array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    RecordBatch, StringArray, StringBuilder,
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
            PG_CATALOG_TABLE_PG_PROC => {
                let table = Arc::new(PgProcTable::new(self.catalog_list.clone()));
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

    /// Create pg_type table with PostgreSQL type definitions
    fn create_pg_type_table(&self) -> Arc<dyn TableProvider> {
        // Define complete schema for pg_type
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),    // Type OID
            Field::new("typname", DataType::Utf8, false), // Type name
            Field::new("typnamespace", DataType::Int32, false), // Namespace OID
            Field::new("typowner", DataType::Int32, false), // Owner OID
            Field::new("typlen", DataType::Int16, false), // Type length (-1 for variable)
            Field::new("typbyval", DataType::Boolean, false), // Passed by value?
            Field::new("typtype", DataType::Utf8, false), // Type category (b=base, c=composite, etc)
            Field::new("typcategory", DataType::Utf8, false), // General category
            Field::new("typispreferred", DataType::Boolean, false), // Preferred type in category?
            Field::new("typisdefined", DataType::Boolean, false), // Type is defined?
            Field::new("typdelim", DataType::Utf8, false), // Array element delimiter
            Field::new("typrelid", DataType::Int32, false), // Related pg_class OID (0 if not composite)
            Field::new("typsubscript", DataType::Int32, false), // Subscript handler function OID
            Field::new("typelem", DataType::Int32, false), // Array element type OID (0 if not array)
            Field::new("typarray", DataType::Int32, false), // Array type OID (0 if no array type)
            Field::new("typinput", DataType::Int32, false), // Input function OID
            Field::new("typoutput", DataType::Int32, false), // Output function OID
            Field::new("typreceive", DataType::Int32, false), // Binary input function OID
            Field::new("typsend", DataType::Int32, false), // Binary output function OID
            Field::new("typmodin", DataType::Int32, false), // Type modifier input function OID
            Field::new("typmodout", DataType::Int32, false), // Type modifier output function OID
            Field::new("typanalyze", DataType::Int32, false), // Analyze function OID
            Field::new("typalign", DataType::Utf8, false), // Alignment requirement
            Field::new("typstorage", DataType::Utf8, false), // Storage strategy
            Field::new("typnotnull", DataType::Boolean, false), // NOT NULL constraint?
            Field::new("typbasetype", DataType::Int32, false), // Base type OID (for domains)
            Field::new("typtypmod", DataType::Int32, false), // Type modifier for domains
            Field::new("typndims", DataType::Int32, false), // Array dimensions for domains
            Field::new("typcollation", DataType::Int32, false), // Collation OID
            Field::new("typdefaultbin", DataType::Utf8, true), // Default value in nodeToString format
            Field::new("typdefault", DataType::Utf8, true),    // Default value as text
            Field::new("typacl", DataType::Utf8, true),        // Access privileges
        ]));

        // Create the data for common PostgreSQL types
        let batch =
            Self::create_pg_type_data(schema.clone()).expect("Failed to create pg_type data");

        // Create memory table with the data
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

        Arc::new(provider)
    }

    /// Create record batch with PostgreSQL type definitions
    fn create_pg_type_data(schema: SchemaRef) -> Result<RecordBatch> {
        // Define common PostgreSQL types that we use in our mappings
        #[allow(clippy::type_complexity)]
        let types: Vec<(
            i32,
            &str,
            i32,
            i32,
            i16,
            bool,
            &str,
            &str,
            bool,
            bool,
            &str,
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
            &str,
            &str,
            bool,
            i32,
            i32,
            i32,
            i32,
            Option<&str>,
            Option<&str>,
            Option<&str>,
        )> = vec![
            // Basic types
            (
                16, "bool", 11, 10, 1, true, "b", "B", true, true, ",", 0, 0, 0, 1000, 1242, 1243,
                2556, 2557, 0, 0, 0, "c", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                21, "int2", 11, 10, 2, true, "b", "N", false, true, ",", 0, 0, 0, 1005, 1242, 1243,
                2562, 2563, 0, 0, 0, "s", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                23, "int4", 11, 10, 4, true, "b", "N", true, true, ",", 0, 0, 0, 1007, 1242, 1243,
                2562, 2563, 0, 0, 0, "i", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                20, "int8", 11, 10, 8, false, "b", "N", false, true, ",", 0, 0, 0, 1016, 1242,
                1243, 2562, 2563, 0, 0, 0, "d", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                700, "float4", 11, 10, 4, true, "b", "N", false, true, ",", 0, 0, 0, 1021, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                701, "float8", 11, 10, 8, false, "b", "N", true, true, ",", 0, 0, 0, 1022, 1242,
                1243, 2562, 2563, 0, 0, 0, "d", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1043, "varchar", 11, 10, -1, false, "b", "S", false, true, ",", 0, 0, 0, 1015,
                1242, 1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 100, None, None, None,
            ),
            (
                25, "text", 11, 10, -1, false, "b", "S", true, true, ",", 0, 0, 0, 1009, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 100, None, None, None,
            ),
            (
                1082, "date", 11, 10, 4, true, "b", "D", false, true, ",", 0, 0, 0, 1182, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1114,
                "timestamp",
                11,
                10,
                8,
                false,
                "b",
                "D",
                false,
                true,
                ",",
                0,
                0,
                0,
                1115,
                1242,
                1243,
                2562,
                2563,
                0,
                0,
                0,
                "d",
                "p",
                false,
                0,
                -1,
                0,
                0,
                None,
                None,
                None,
            ),
            (
                1184,
                "timestamptz",
                11,
                10,
                8,
                false,
                "b",
                "D",
                true,
                true,
                ",",
                0,
                0,
                0,
                1185,
                1242,
                1243,
                2562,
                2563,
                0,
                0,
                0,
                "d",
                "p",
                false,
                0,
                -1,
                0,
                0,
                None,
                None,
                None,
            ),
            (
                1083, "time", 11, 10, 8, false, "b", "D", false, true, ",", 0, 0, 0, 1183, 1242,
                1243, 2562, 2563, 0, 0, 0, "d", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1186, "interval", 11, 10, 16, false, "b", "T", false, true, ",", 0, 0, 0, 1187,
                1242, 1243, 2562, 2563, 0, 0, 0, "d", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                17, "bytea", 11, 10, -1, false, "b", "U", false, true, ",", 0, 0, 0, 1001, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1700, "numeric", 11, 10, -1, false, "b", "N", false, true, ",", 0, 0, 0, 1231,
                1242, 1243, 2562, 2563, 0, 0, 0, "i", "m", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                18, "char", 11, 10, 1, true, "b", "S", false, true, ",", 0, 0, 0, 1002, 1242, 1243,
                2562, 2563, 0, 0, 0, "c", "p", false, 0, -1, 0, 100, None, None, None,
            ),
            (
                705, "unknown", 11, 10, -2, false, "p", "X", false, true, ",", 0, 0, 0, 0, 1242,
                1243, 2562, 2563, 0, 0, 0, "c", "p", false, 0, -1, 0, 0, None, None, None,
            ),
            // Array types
            (
                1000, "_bool", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 16, 0, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1005, "_int2", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 21, 0, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1007, "_int4", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 23, 0, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1016, "_int8", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 20, 0, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1021, "_float4", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 700, 0,
                1242, 1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1022, "_float8", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 701, 0,
                1242, 1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1015, "_varchar", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 1043, 0,
                1242, 1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1009, "_text", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 25, 0, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1182, "_date", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 1082, 0,
                1242, 1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1115,
                "_timestamp",
                11,
                10,
                -1,
                false,
                "b",
                "A",
                false,
                true,
                ",",
                0,
                2750,
                1114,
                0,
                1242,
                1243,
                2562,
                2563,
                0,
                0,
                0,
                "i",
                "x",
                false,
                0,
                -1,
                0,
                0,
                None,
                None,
                None,
            ),
            (
                1185,
                "_timestamptz",
                11,
                10,
                -1,
                false,
                "b",
                "A",
                false,
                true,
                ",",
                0,
                2750,
                1184,
                0,
                1242,
                1243,
                2562,
                2563,
                0,
                0,
                0,
                "i",
                "x",
                false,
                0,
                -1,
                0,
                0,
                None,
                None,
                None,
            ),
            (
                1183, "_time", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 1083, 0,
                1242, 1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1187,
                "_interval",
                11,
                10,
                -1,
                false,
                "b",
                "A",
                false,
                true,
                ",",
                0,
                2750,
                1186,
                0,
                1242,
                1243,
                2562,
                2563,
                0,
                0,
                0,
                "i",
                "x",
                false,
                0,
                -1,
                0,
                0,
                None,
                None,
                None,
            ),
            (
                1001, "_bytea", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 17, 0,
                1242, 1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1231, "_numeric", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 1700, 0,
                1242, 1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
            (
                1002, "_char", 11, 10, -1, false, "b", "A", false, true, ",", 0, 2750, 18, 0, 1242,
                1243, 2562, 2563, 0, 0, 0, "i", "x", false, 0, -1, 0, 0, None, None, None,
            ),
        ];

        // Convert to Arrow arrays
        let mut oids = Vec::new();
        let mut typnames = Vec::new();
        let mut typnamespaces = Vec::new();
        let mut typowners = Vec::new();
        let mut typlens = Vec::new();
        let mut typbyvals = Vec::new();
        let mut typtypes = Vec::new();
        let mut typcategories = Vec::new();
        let mut typispreferreds = Vec::new();
        let mut typisdefineds = Vec::new();
        let mut typdelims = Vec::new();
        let mut typrelids = Vec::new();
        let mut typsubscripts = Vec::new();
        let mut typelems = Vec::new();
        let mut typarrays = Vec::new();
        let mut typinputs = Vec::new();
        let mut typoutputs = Vec::new();
        let mut typreceives = Vec::new();
        let mut typsends = Vec::new();
        let mut typmodins = Vec::new();
        let mut typmodouts = Vec::new();
        let mut typanalyzes = Vec::new();
        let mut typaligns = Vec::new();
        let mut typstorages = Vec::new();
        let mut typnotnulls = Vec::new();
        let mut typbasetypes = Vec::new();
        let mut typtypemods = Vec::new();
        let mut typndimss = Vec::new();
        let mut typcollations = Vec::new();
        let mut typdefaultbins: Vec<Option<String>> = Vec::new();
        let mut typdefaults: Vec<Option<String>> = Vec::new();
        let mut typacls: Vec<Option<String>> = Vec::new();

        for (
            oid,
            typname,
            typnamespace,
            typowner,
            typlen,
            typbyval,
            typtype,
            typcategory,
            typispreferred,
            typisdefined,
            typdelim,
            typrelid,
            typsubscript,
            typelem,
            typarray,
            typinput,
            typoutput,
            typreceive,
            typsend,
            typmodin,
            typmodout,
            typanalyze,
            typalign,
            typstorage,
            typnotnull,
            typbasetype,
            typtypemod,
            typndims,
            typcollation,
            typdefaultbin,
            typdefault,
            typacl,
        ) in types
        {
            oids.push(oid);
            typnames.push(typname.to_string());
            typnamespaces.push(typnamespace);
            typowners.push(typowner);
            typlens.push(typlen);
            typbyvals.push(typbyval);
            typtypes.push(typtype.to_string());
            typcategories.push(typcategory.to_string());
            typispreferreds.push(typispreferred);
            typisdefineds.push(typisdefined);
            typdelims.push(typdelim.to_string());
            typrelids.push(typrelid);
            typsubscripts.push(typsubscript);
            typelems.push(typelem);
            typarrays.push(typarray);
            typinputs.push(typinput);
            typoutputs.push(typoutput);
            typreceives.push(typreceive);
            typsends.push(typsend);
            typmodins.push(typmodin);
            typmodouts.push(typmodout);
            typanalyzes.push(typanalyze);
            typaligns.push(typalign.to_string());
            typstorages.push(typstorage.to_string());
            typnotnulls.push(typnotnull);
            typbasetypes.push(typbasetype);
            typtypemods.push(typtypemod);
            typndimss.push(typndims);
            typcollations.push(typcollation);
            typdefaultbins.push(typdefaultbin.map(|s| s.to_string()));
            typdefaults.push(typdefault.map(|s| s.to_string()));
            typacls.push(typacl.map(|s| s.to_string()));
        }

        // Create Arrow arrays
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(typnames)),
            Arc::new(Int32Array::from(typnamespaces)),
            Arc::new(Int32Array::from(typowners)),
            Arc::new(Int16Array::from(typlens)),
            Arc::new(BooleanArray::from(typbyvals)),
            Arc::new(StringArray::from(typtypes)),
            Arc::new(StringArray::from(typcategories)),
            Arc::new(BooleanArray::from(typispreferreds)),
            Arc::new(BooleanArray::from(typisdefineds)),
            Arc::new(StringArray::from(typdelims)),
            Arc::new(Int32Array::from(typrelids)),
            Arc::new(Int32Array::from(typsubscripts)),
            Arc::new(Int32Array::from(typelems)),
            Arc::new(Int32Array::from(typarrays)),
            Arc::new(Int32Array::from(typinputs)),
            Arc::new(Int32Array::from(typoutputs)),
            Arc::new(Int32Array::from(typreceives)),
            Arc::new(Int32Array::from(typsends)),
            Arc::new(Int32Array::from(typmodins)),
            Arc::new(Int32Array::from(typmodouts)),
            Arc::new(Int32Array::from(typanalyzes)),
            Arc::new(StringArray::from(typaligns)),
            Arc::new(StringArray::from(typstorages)),
            Arc::new(BooleanArray::from(typnotnulls)),
            Arc::new(Int32Array::from(typbasetypes)),
            Arc::new(Int32Array::from(typtypemods)),
            Arc::new(Int32Array::from(typndimss)),
            Arc::new(Int32Array::from(typcollations)),
            Arc::new(StringArray::from_iter(typdefaultbins.into_iter())),
            Arc::new(StringArray::from_iter(typdefaults.into_iter())),
            Arc::new(StringArray::from_iter(typacls.into_iter())),
        ];

        Ok(RecordBatch::try_new(schema, arrays)?)
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

pub fn create_version_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Create a UTF8 array with DataFusion-Postgres version info
        let mut builder = StringBuilder::new();
        builder.append_value("DataFusion-Postgres 0.5.1, compiled with DataFusion 47.0.0");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "version",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_pg_version_num_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // PostgreSQL version number format: major * 10000 + minor * 100 + patch
        // We'll use a fake version 16.1.0 for compatibility
        let mut builder = StringBuilder::new();
        builder.append_value("160100");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "pg_version_num",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_current_database_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Return the default catalog name
        let mut builder = StringBuilder::new();
        builder.append_value("datafusion");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_database",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_current_user_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Return a default user name
        let mut builder = StringBuilder::new();
        builder.append_value("postgres");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_user",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_session_user_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Return a default user name
        let mut builder = StringBuilder::new();
        builder.append_value("postgres");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "session_user",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_user_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Return a default user name
        let mut builder = StringBuilder::new();
        builder.append_value("postgres");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "user",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_pg_encoding_to_char_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let _encoding_id = &args[0]; // We'll ignore the input and always return UTF8

        // Always return UTF8 as the encoding
        let mut builder = StringBuilder::new();
        builder.append_value("UTF8");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "pg_encoding_to_char",
        vec![DataType::Int32],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_getdatabaseencoding_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Always return UTF8 as the encoding
        let mut builder = StringBuilder::new();
        builder.append_value("UTF8");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "getdatabaseencoding",
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

    // Register system information functions
    session_context.register_udf(create_version_udf());
    session_context.register_udf(create_pg_version_num_udf());

    // Register database/user functions
    session_context.register_udf(create_current_database_udf());
    session_context.register_udf(create_current_user_udf());
    session_context.register_udf(create_session_user_udf());
    session_context.register_udf(create_user_udf());

    // Register encoding functions
    session_context.register_udf(create_pg_encoding_to_char_udf());
    session_context.register_udf(create_getdatabaseencoding_udf());

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
                                        .unwrap_or(pgwire::api::Type::UNKNOWN);

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

#[derive(Debug)]
struct PgProcTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgProcTable {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
        // Define the schema for pg_proc
        // This matches key columns from PostgreSQL's pg_proc
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Function OID
            Field::new("proname", DataType::Utf8, false), // Function name
            Field::new("pronamespace", DataType::Int32, false), // Namespace OID
            Field::new("proowner", DataType::Int32, false), // Owner OID
            Field::new("prolang", DataType::Int32, false), // Language OID
            Field::new("procost", DataType::Float32, false), // Execution cost
            Field::new("prorows", DataType::Float32, false), // Number of result rows
            Field::new("provariadic", DataType::Int32, false), // Variadic array parameter type
            Field::new("prosupport", DataType::Int32, false), // Support function OID
            Field::new("prokind", DataType::Utf8, false), // Function kind (f=function, p=procedure, etc.)
            Field::new("prosecdef", DataType::Boolean, false), // Security definer?
            Field::new("proleakproof", DataType::Boolean, false), // Leak proof?
            Field::new("proisstrict", DataType::Boolean, false), // Strict (returns null on null input)?
            Field::new("proretset", DataType::Boolean, false),   // Returns a set?
            Field::new("provolatile", DataType::Utf8, false), // Volatility (i=immutable, s=stable, v=volatile)
            Field::new("proparallel", DataType::Utf8, false), // Parallel safety (s=safe, r=restricted, u=unsafe)
            Field::new("pronargs", DataType::Int16, false),   // Number of input arguments
            Field::new("pronargdefaults", DataType::Int16, false), // Number of arguments with defaults
            Field::new("prorettype", DataType::Int32, false),      // Return type OID
            Field::new("proargtypes", DataType::Utf8, false), // Argument types (simplified as string)
            Field::new("proallargtypes", DataType::Utf8, true), // All argument types including OUT
            Field::new("proargmodes", DataType::Utf8, true), // Argument modes (i=IN, o=OUT, b=INOUT, v=VARIADIC)
            Field::new("proargnames", DataType::Utf8, true), // Argument names
            Field::new("proargdefaults", DataType::Utf8, true), // Default values for arguments
            Field::new("protrftypes", DataType::Utf8, true), // Transform function type OIDs
            Field::new("prosrc", DataType::Utf8, false),     // Function source code
            Field::new("probin", DataType::Utf8, true),      // Binary file name for C functions
            Field::new("prosqlbody", DataType::Utf8, true),  // SQL body for SQL-language functions
            Field::new("proconfig", DataType::Utf8, true), // Function-local configuration settings
            Field::new("proacl", DataType::Utf8, true),    // Access privileges
        ]));

        Self {
            schema,
            catalog_list,
        }
    }

    /// Generate record batches based on available functions
    async fn get_data(
        schema: SchemaRef,
        _catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Result<RecordBatch> {
        // Define common PostgreSQL system functions that we support
        #[allow(clippy::type_complexity)]
        let functions: Vec<(
            i32,
            &str,
            i32,
            i32,
            i32,
            f32,
            f32,
            i32,
            i32,
            &str,
            bool,
            bool,
            bool,
            bool,
            &str,
            &str,
            i16,
            i16,
            i32,
            &str,
            Option<&str>,
            Option<&str>,
            Option<&str>,
            Option<&str>,
            Option<&str>,
            &str,
            Option<&str>,
            Option<&str>,
            Option<&str>,
            Option<&str>,
        )> = vec![
            // System information functions
            (
                2200,
                "version",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                0,
                0,
                25,
                "",
                None,
                None,
                None,
                None,
                None,
                "SELECT 'DataFusion-Postgres 0.5.1'",
                None,
                None,
                None,
                None,
            ),
            (
                2201,
                "pg_version_num",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                0,
                0,
                25,
                "",
                None,
                None,
                None,
                None,
                None,
                "SELECT '160100'",
                None,
                None,
                None,
                None,
            ),
            // Database/user functions
            (
                2202,
                "current_database",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                0,
                0,
                25,
                "",
                None,
                None,
                None,
                None,
                None,
                "SELECT 'datafusion'",
                None,
                None,
                None,
                None,
            ),
            (
                2203,
                "current_user",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                0,
                0,
                25,
                "",
                None,
                None,
                None,
                None,
                None,
                "SELECT 'postgres'",
                None,
                None,
                None,
                None,
            ),
            (
                2204,
                "session_user",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                0,
                0,
                25,
                "",
                None,
                None,
                None,
                None,
                None,
                "SELECT 'postgres'",
                None,
                None,
                None,
                None,
            ),
            (
                2205,
                "user",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                0,
                0,
                25,
                "",
                None,
                None,
                None,
                None,
                None,
                "SELECT 'postgres'",
                None,
                None,
                None,
                None,
            ),
            // Schema functions
            (
                2206,
                "current_schema",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                0,
                0,
                25,
                "",
                None,
                None,
                None,
                None,
                None,
                "SELECT 'public'",
                None,
                None,
                None,
                None,
            ),
            (
                2207,
                "current_schemas",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                1,
                0,
                1009,
                "16",
                None,
                None,
                Some("implicit"),
                None,
                None,
                "SELECT ARRAY['public']",
                None,
                None,
                None,
                None,
            ),
            // Encoding functions
            (
                2208,
                "getdatabaseencoding",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                0,
                0,
                25,
                "",
                None,
                None,
                None,
                None,
                None,
                "SELECT 'UTF8'",
                None,
                None,
                None,
                None,
            ),
            (
                2209,
                "pg_encoding_to_char",
                11,
                10,
                12,
                1.0,
                0.0,
                0,
                0,
                "f",
                false,
                true,
                true,
                false,
                "i",
                "s",
                1,
                0,
                25,
                "23",
                None,
                None,
                Some("encoding_id"),
                None,
                None,
                "SELECT 'UTF8'",
                None,
                None,
                None,
                None,
            ),
        ];

        // Convert to Arrow arrays
        let mut oids = Vec::new();
        let mut pronames = Vec::new();
        let mut pronamespaces = Vec::new();
        let mut proowners = Vec::new();
        let mut prolangs = Vec::new();
        let mut procosts = Vec::new();
        let mut prorows_vec = Vec::new();
        let mut provariadics = Vec::new();
        let mut prosupports = Vec::new();
        let mut prokinds = Vec::new();
        let mut prosecdefs = Vec::new();
        let mut proleakproofs = Vec::new();
        let mut proisstricts = Vec::new();
        let mut proretsets = Vec::new();
        let mut provolatiles = Vec::new();
        let mut proparallels = Vec::new();
        let mut pronargs_vec = Vec::new();
        let mut pronargdefaults_vec = Vec::new();
        let mut prorettypes = Vec::new();
        let mut proargtypes_vec = Vec::new();
        let mut proallargtypes: Vec<Option<String>> = Vec::new();
        let mut proargmodes: Vec<Option<String>> = Vec::new();
        let mut proargnames: Vec<Option<String>> = Vec::new();
        let mut proargdefaults: Vec<Option<String>> = Vec::new();
        let mut protrftypes: Vec<Option<String>> = Vec::new();
        let mut prosrcs = Vec::new();
        let mut probins: Vec<Option<String>> = Vec::new();
        let mut prosqlbodies: Vec<Option<String>> = Vec::new();
        let mut proconfigs: Vec<Option<String>> = Vec::new();
        let mut proacls: Vec<Option<String>> = Vec::new();

        for (
            oid,
            proname,
            pronamespace,
            proowner,
            prolang,
            procost,
            prorows,
            provariadic,
            prosupport,
            prokind,
            prosecdef,
            proleakproof,
            proisstrict,
            proretset,
            provolatile,
            proparallel,
            pronargs,
            pronargdefaults,
            prorettype,
            proargtypes,
            proallargtype,
            proargmode,
            proargname,
            proargdefault,
            protrttype,
            prosrc,
            probin,
            prosqlbody,
            proconfig,
            proacl,
        ) in functions
        {
            oids.push(oid);
            pronames.push(proname.to_string());
            pronamespaces.push(pronamespace);
            proowners.push(proowner);
            prolangs.push(prolang);
            procosts.push(procost);
            prorows_vec.push(prorows);
            provariadics.push(provariadic);
            prosupports.push(prosupport);
            prokinds.push(prokind.to_string());
            prosecdefs.push(prosecdef);
            proleakproofs.push(proleakproof);
            proisstricts.push(proisstrict);
            proretsets.push(proretset);
            provolatiles.push(provolatile.to_string());
            proparallels.push(proparallel.to_string());
            pronargs_vec.push(pronargs);
            pronargdefaults_vec.push(pronargdefaults);
            prorettypes.push(prorettype);
            proargtypes_vec.push(proargtypes.to_string());
            proallargtypes.push(proallargtype.map(|s| s.to_string()));
            proargmodes.push(proargmode.map(|s| s.to_string()));
            proargnames.push(proargname.map(|s| s.to_string()));
            proargdefaults.push(proargdefault.map(|s| s.to_string()));
            protrftypes.push(protrttype.map(|s| s.to_string()));
            prosrcs.push(prosrc.to_string());
            probins.push(probin.map(|s| s.to_string()));
            prosqlbodies.push(prosqlbody.map(|s| s.to_string()));
            proconfigs.push(proconfig.map(|s| s.to_string()));
            proacls.push(proacl.map(|s| s.to_string()));
        }

        // Create Arrow arrays
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(pronames)),
            Arc::new(Int32Array::from(pronamespaces)),
            Arc::new(Int32Array::from(proowners)),
            Arc::new(Int32Array::from(prolangs)),
            Arc::new(Float32Array::from(procosts)),
            Arc::new(Float32Array::from(prorows_vec)),
            Arc::new(Int32Array::from(provariadics)),
            Arc::new(Int32Array::from(prosupports)),
            Arc::new(StringArray::from(prokinds)),
            Arc::new(BooleanArray::from(prosecdefs)),
            Arc::new(BooleanArray::from(proleakproofs)),
            Arc::new(BooleanArray::from(proisstricts)),
            Arc::new(BooleanArray::from(proretsets)),
            Arc::new(StringArray::from(provolatiles)),
            Arc::new(StringArray::from(proparallels)),
            Arc::new(Int16Array::from(pronargs_vec)),
            Arc::new(Int16Array::from(pronargdefaults_vec)),
            Arc::new(Int32Array::from(prorettypes)),
            Arc::new(StringArray::from(proargtypes_vec)),
            Arc::new(StringArray::from_iter(proallargtypes.into_iter())),
            Arc::new(StringArray::from_iter(proargmodes.into_iter())),
            Arc::new(StringArray::from_iter(proargnames.into_iter())),
            Arc::new(StringArray::from_iter(proargdefaults.into_iter())),
            Arc::new(StringArray::from_iter(protrftypes.into_iter())),
            Arc::new(StringArray::from(prosrcs)),
            Arc::new(StringArray::from_iter(probins.into_iter())),
            Arc::new(StringArray::from_iter(prosqlbodies.into_iter())),
            Arc::new(StringArray::from_iter(proconfigs.into_iter())),
            Arc::new(StringArray::from_iter(proacls.into_iter())),
        ];

        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

impl PartitionStream for PgProcTable {
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
