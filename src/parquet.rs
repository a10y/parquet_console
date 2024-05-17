use parquet::{
    data_type::{ByteArray, FixedLenByteArray},
    file::reader::{ChunkReader, FileReader, SerializedFileReader},
};
use parquet2::{
    schema::types::PhysicalType,
    statistics::{BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics},
    types::NativeType,
};

pub trait PhysicalTypeExt {
    fn human_readable(&self) -> &'static str;
}

impl PhysicalTypeExt for PhysicalType {
    fn human_readable(&self) -> &'static str {
        match *self {
            parquet2::schema::types::PhysicalType::Boolean => "BOOLEAN",
            parquet2::schema::types::PhysicalType::Int32 => "INT32",
            parquet2::schema::types::PhysicalType::Int64 => "INT64",
            parquet2::schema::types::PhysicalType::Int96 => "INT96",
            parquet2::schema::types::PhysicalType::Float => "FLOAT",
            parquet2::schema::types::PhysicalType::Double => "DOUBLE",
            parquet2::schema::types::PhysicalType::ByteArray => "BYTEARRAY",
            parquet2::schema::types::PhysicalType::FixedLenByteArray(_) => "FIXED_LEN_BYTEARRAY",
        }
    }
}

/// Type-erased variant of parquet2's [Statistics] type.
/// This is meant to be a human-visible wrapper that allows printing of stats in the most
/// understandable format.
///
/// All PhysicalTypes are supported except for INT96.
#[derive(Debug, Default, Clone)]
pub struct HumanFriendlyStats {
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: Option<i64>,
    pub distinct_values: Option<i64>,
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
/// This is meant to make it very easy to extract out relevant information from a column chunk.
pub trait ColumnChunkMetaDataExt {
    fn stats(self) -> HumanFriendlyStats;
}

impl ColumnChunkMetaDataExt for &parquet2::metadata::ColumnChunkMetaData {
    fn stats(self) -> HumanFriendlyStats {
        let stats: HumanFriendlyStats = match self.physical_type() {
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

        stats
    }
}

/// Read a sample of values from the column chunk. Or just read the individual values from it.
///
/// Returns a Stringified sample of column value that we can display.
pub fn sample_column<R: ChunkReader + 'static>(
    chunk_reader: R,
    row_group: usize,
    column_chunk: usize,
) -> String {
    // How can you read a batch of records from a single ColumnChunk?
    // Find a way to deploy using the native type here.
    let file_reader = SerializedFileReader::new(chunk_reader).unwrap();
    let mut column_reader = file_reader
        .get_row_group(row_group)
        .unwrap()
        .get_column_reader(column_chunk)
        .unwrap();

    let mut def_levels: Vec<i16> = Vec::new();
    let mut rep_levels: Vec<i16> = Vec::new();

    match column_reader {
        parquet::column::reader::ColumnReader::BoolColumnReader(ref mut bool_reader) => {
            let mut values_vec: Vec<bool> = Vec::new();
            let (complete, non_null, _) = bool_reader
                .read_records(
                    10,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values_vec,
                )
                .unwrap();

            let sample = values_vec
                .iter()
                .take(10)
                .map(|b| b.to_string())
                .collect::<Vec<_>>();

            format!(
                "count: {}, non-null: {} sample: {:?}",
                complete, non_null, &sample
            )
        }
        parquet::column::reader::ColumnReader::Int32ColumnReader(ref mut int32_reader) => {
            let mut values_vec: Vec<i32> = Vec::new();
            let (complete, non_null, _) = int32_reader
                .read_records(
                    10,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values_vec,
                )
                .unwrap();

            let sample = values_vec
                .iter()
                .take(10)
                .map(|i| i.to_string())
                .collect::<Vec<_>>();

            format!(
                "count: {}, non-null: {} sample: {:?}",
                complete, non_null, &sample
            )
        }
        parquet::column::reader::ColumnReader::Int64ColumnReader(ref mut int64_reader) => {
            let mut values_vec: Vec<i64> = Vec::new();
            let (complete, non_null, _) = int64_reader
                .read_records(
                    10,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values_vec,
                )
                .unwrap();

            let sample = values_vec
                .iter()
                .take(10)
                .map(|i| i.to_string())
                .collect::<Vec<_>>();

            format!(
                "count: {}, non-null: {} sample: {:?}",
                complete, non_null, &sample
            )
        }
        parquet::column::reader::ColumnReader::Int96ColumnReader(_) => {
            "INT96 sampling not supported".to_string()
        }
        parquet::column::reader::ColumnReader::FloatColumnReader(ref mut float32_reader) => {
            let mut values_vec: Vec<f32> = Vec::new();
            let (complete, non_null, _) = float32_reader
                .read_records(
                    10,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values_vec,
                )
                .unwrap();

            let sample = values_vec
                .iter()
                .take(10)
                .map(|i| i.to_string())
                .collect::<Vec<_>>();

            format!(
                "count: {}, non-null: {} sample: {:?}",
                complete, non_null, &sample
            )
        }
        parquet::column::reader::ColumnReader::DoubleColumnReader(ref mut float64_reader) => {
            let mut values_vec: Vec<f64> = Vec::new();
            let (complete, non_null, _) = float64_reader
                .read_records(
                    10,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values_vec,
                )
                .unwrap();

            let sample = values_vec
                .iter()
                .take(10)
                .map(|i| i.to_string())
                .collect::<Vec<_>>();

            format!(
                "count: {}, non-null: {} sample: {:?}",
                complete, non_null, &sample
            )
        }
        parquet::column::reader::ColumnReader::ByteArrayColumnReader(ref mut bytearray_reader) => {
            let mut values_vec: Vec<ByteArray> = Vec::new();
            let (complete, non_null, _) = bytearray_reader
                .read_records(
                    10,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values_vec,
                )
                .unwrap();

            let sample = values_vec
                .iter()
                .take(10)
                .map(|i| i.to_string())
                .collect::<Vec<_>>();

            format!(
                "count: {}, non-null: {} sample: {:?}",
                complete, non_null, &sample
            )
        }
        parquet::column::reader::ColumnReader::FixedLenByteArrayColumnReader(
            ref mut fixedlen_reader,
        ) => {
            let mut values_vec: Vec<FixedLenByteArray> = Vec::new();
            let (complete, non_null, _) = fixedlen_reader
                .read_records(
                    10,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values_vec,
                )
                .unwrap();

            let sample = values_vec
                .iter()
                .take(10)
                .map(|i| i.to_string())
                .collect::<Vec<_>>();

            format!(
                "count: {}, non-null: {} sample: {:?}",
                complete, non_null, &sample
            )
        }
    }
}
