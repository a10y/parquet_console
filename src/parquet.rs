use parquet2::{
    schema::types::PhysicalType,
    statistics::{BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics},
    types::NativeType,
};
use ratatui::{style::Stylize, text::Line};

pub trait PhysicalTypeExt {
    fn human_readable(&self) -> &'static str;
}

impl PhysicalTypeExt for PhysicalType {
    fn human_readable(&self) -> &'static str {
        match self {
            &parquet2::schema::types::PhysicalType::Boolean => "BOOLEAN",
            &parquet2::schema::types::PhysicalType::Int32 => "INT32",
            &parquet2::schema::types::PhysicalType::Int64 => "INT64",
            &parquet2::schema::types::PhysicalType::Int96 => "INT96",
            &parquet2::schema::types::PhysicalType::Float => "FLOAT",
            &parquet2::schema::types::PhysicalType::Double => "DOUBLE",
            &parquet2::schema::types::PhysicalType::ByteArray => "BYTEARRAY",
            &parquet2::schema::types::PhysicalType::FixedLenByteArray(_) => "FIXED_LEN_BYTEARRAY",
        }
    }
}

/// Type-erased variant of parquet2's [Statistics] type.
/// This is meant to be a human-visible wrapper that allows printing of stats in the most
/// understandable format.
///
/// All PhysicalTypes are supported except for INT96.
#[derive(Debug, Default, Clone)]
pub(crate) struct HumanFriendlyStats {
    pub(crate) min: Option<String>,
    pub(crate) max: Option<String>,
    pub(crate) null_count: Option<i64>,
    pub(crate) distinct_values: Option<i64>,
}

impl<T: NativeType> From<&PrimitiveStatistics<T>> for HumanFriendlyStats {
    fn from(value: &PrimitiveStatistics<T>) -> Self {
        let min = value.min_value.map(|min_value| format!("{:?}", min_value));
        let max = value.max_value.map(|max_value| format!("{:?}", max_value));
        let null_count = value.null_count;

        Self {
            min,
            max,
            null_count,
            distinct_values: None,
        }
    }
}

impl From<&BooleanStatistics> for HumanFriendlyStats {
    fn from(value: &BooleanStatistics) -> Self {
        let min = value.min_value.map(|min_value| format!("{:?}", min_value));
        let max = value.max_value.map(|max_value| format!("{:?}", max_value));
        let null_count = value.null_count;
        let distinct_values = value.distinct_count;

        Self {
            min,
            max,
            null_count,
            distinct_values,
        }
    }
}

impl From<&BinaryStatistics> for HumanFriendlyStats {
    fn from(value: &BinaryStatistics) -> Self {
        let min = value
            .min_value
            .clone()
            .map(|min_value| String::from_utf8(min_value).unwrap_or("UNK".to_string()));
        let max = value
            .max_value
            .clone()
            .map(|min_value| String::from_utf8(min_value).unwrap_or("UNK".to_string()));
        let null_count = value.null_count;
        let distinct_values = value.distinct_count;

        Self {
            min,
            max,
            null_count,
            distinct_values,
        }
    }
}

impl From<&FixedLenStatistics> for HumanFriendlyStats {
    fn from(value: &FixedLenStatistics) -> Self {
        let min = value
            .min_value
            .clone()
            .map(|min_value| String::from_utf8(min_value).unwrap_or("UNK".to_string()));
        let max = value
            .max_value
            .clone()
            .map(|min_value| String::from_utf8(min_value).unwrap_or("UNK".to_string()));
        let null_count = value.null_count;
        let distinct_values = value.distinct_count;

        Self {
            min,
            max,
            null_count,
            distinct_values,
        }
    }
}

/// Extension trait that turns a parquet2 ColumnChunkMetadata into a list of viewable elements
/// This is meant to make it very easy to extract out relevant information from a column chunk instead of
/// attacking all of these things.
pub trait ColumnChunkMetaDataExt {
    fn view(self) -> Vec<Line<'static>>;
}

impl ColumnChunkMetaDataExt for &parquet2::metadata::ColumnChunkMetaData {
    fn view(self) -> Vec<Line<'static>> {
        let column_chunk = self.column_chunk().meta_data.clone().unwrap();
        let phys_typ = self.physical_type().human_readable();
        let min_max_stats: HumanFriendlyStats = match self.physical_type() {
            parquet2::schema::types::PhysicalType::Boolean => HumanFriendlyStats::default(),
            parquet2::schema::types::PhysicalType::Int32 => self
                .statistics()
                .map(|stats| {
                    stats
                        .unwrap()
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<i32>>()
                        .cloned()
                })
                .and_then(|stats| stats.map(|s| HumanFriendlyStats::from(&s)))
                .unwrap_or_default(),
            parquet2::schema::types::PhysicalType::Int64 => self
                .statistics()
                .map(|stats| {
                    stats
                        .unwrap()
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<i64>>()
                        .cloned()
                })
                .and_then(|stats| stats.map(|s| HumanFriendlyStats::from(&s)))
                .unwrap_or_default(),
            parquet2::schema::types::PhysicalType::Int96 => HumanFriendlyStats::default(),
            parquet2::schema::types::PhysicalType::Float => self
                .statistics()
                .map(|stats| {
                    stats
                        .unwrap()
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<f32>>()
                        .cloned()
                })
                .and_then(|stats| stats.map(|s| HumanFriendlyStats::from(&s)))
                .unwrap_or_default(),
            parquet2::schema::types::PhysicalType::Double => self
                .statistics()
                .map(|stats| {
                    stats
                        .unwrap()
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<f64>>()
                        .cloned()
                })
                .and_then(|stats| stats.map(|s| HumanFriendlyStats::from(&s)))
                .unwrap_or_default(),
            parquet2::schema::types::PhysicalType::ByteArray => self
                .statistics()
                .map(|stats| {
                    stats
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BinaryStatistics>()
                        .cloned()
                })
                .and_then(|stats| stats.map(|s| HumanFriendlyStats::from(&s)))
                .unwrap_or_default(),
            parquet2::schema::types::PhysicalType::FixedLenByteArray(_) => self
                .statistics()
                .map(|stats| {
                    stats
                        .unwrap()
                        .as_any()
                        .downcast_ref::<FixedLenStatistics>()
                        .cloned()
                })
                .and_then(|stats| stats.map(|s| HumanFriendlyStats::from(&s)))
                .unwrap_or_default(),
        };

        vec![
            // Column name
            Line::from(format!("Column: {}", column_chunk.path_in_schema.join(".")))
                .bold()
                .green()
                .underlined(),
            // Typing
            Line::from(format!("Physical Type: {}", phys_typ)),
            // Statistics
            Line::from(format!("Stats={:?}", min_max_stats)).yellow(),
        ]
    }
}
