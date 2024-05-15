use parquet2::{
    metadata::ColumnChunkMetaData,
    schema::types::PhysicalType,
    statistics::{BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics},
};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Paragraph, Widget},
};

use crate::{
    parquet::{HumanFriendlyStats, PhysicalTypeExt},
    ActivePane, App,
};

pub struct ColumnChunkSimpleView {
    name: String,
    physical_type: PhysicalType,
}

impl ColumnChunkSimpleView {
    pub fn from(chunk: &ColumnChunkMetaData) -> Self {
        let name = chunk.descriptor().path_in_schema.join(".");
        let physical_type = chunk.physical_type();
        Self {
            name,
            physical_type,
        }
    }
}

impl Widget for &ColumnChunkSimpleView {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer) {
        Line::from(vec![
            Span::from(self.name.as_str()).green().bold(),
            Span::from("//"),
            Span::from(self.physical_type.human_readable()).magenta(),
        ])
        .render(area, buf);
    }
}

pub struct ColumnChunkDetailView {
    name: String,
    physical_type: PhysicalType,
    stats: HumanFriendlyStats,
}

impl ColumnChunkDetailView {
    pub fn from(chunk: &ColumnChunkMetaData) -> Self {
        let name = chunk.descriptor().path_in_schema.join(".");
        let stats = match chunk.physical_type() {
            parquet2::schema::types::PhysicalType::Boolean => chunk
                .statistics()
                .map(|stats| stats.unwrap())
                .and_then(|stats| stats.as_any().downcast_ref::<BooleanStatistics>().cloned())
                .map_or_else(HumanFriendlyStats::default, |boolean_stats| {
                    HumanFriendlyStats::from(&boolean_stats)
                }),
            parquet2::schema::types::PhysicalType::Int32 => chunk
                .statistics()
                .map(|stats| stats.unwrap())
                .and_then(|stats| {
                    stats
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<i32>>()
                        .cloned()
                })
                .map_or_else(HumanFriendlyStats::default, |i32_stats| {
                    HumanFriendlyStats::from(&i32_stats)
                }),
            parquet2::schema::types::PhysicalType::Int64 => chunk
                .statistics()
                .map(|stats| stats.unwrap())
                .and_then(|stats| {
                    stats
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<i64>>()
                        .cloned()
                })
                .map_or_else(HumanFriendlyStats::default, |i64_stats| {
                    HumanFriendlyStats::from(&i64_stats)
                }),
            parquet2::schema::types::PhysicalType::Int96 => HumanFriendlyStats::default(),
            parquet2::schema::types::PhysicalType::Float => chunk
                .statistics()
                .map(|stats| stats.unwrap())
                .and_then(|stats| {
                    stats
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<f32>>()
                        .cloned()
                })
                .map_or_else(HumanFriendlyStats::default, |f32_stats| {
                    HumanFriendlyStats::from(&f32_stats)
                }),
            parquet2::schema::types::PhysicalType::Double => chunk
                .statistics()
                .map(|stats| stats.unwrap())
                .and_then(|stats| {
                    stats
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<f64>>()
                        .cloned()
                })
                .map_or_else(HumanFriendlyStats::default, |f64_stats| {
                    HumanFriendlyStats::from(&f64_stats)
                }),
            parquet2::schema::types::PhysicalType::ByteArray => chunk
                .statistics()
                .map(|stats| stats.unwrap())
                .and_then(|stats| stats.as_any().downcast_ref::<BinaryStatistics>().cloned())
                .map_or_else(HumanFriendlyStats::default, |bin_stats| {
                    HumanFriendlyStats::from(&bin_stats)
                }),
            parquet2::schema::types::PhysicalType::FixedLenByteArray(_) => chunk
                .statistics()
                .map(|stats| stats.unwrap())
                .and_then(|stats| stats.as_any().downcast_ref::<FixedLenStatistics>().cloned())
                .map_or_else(HumanFriendlyStats::default, |fixed_stats| {
                    HumanFriendlyStats::from(&fixed_stats)
                }),
        };

        Self {
            stats,
            name,
            physical_type: chunk.physical_type(),
        }
    }
}

// Pre-compute some of the info that we'd like to deploy here instead
impl Widget for &ColumnChunkDetailView {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer) {
        // Render ourselves directly
        let block = Block::bordered()
            .title(self.name.clone())
            .border_style(Style::new().green());

        let lines: Vec<Line<'static>> = vec![
            Line::from(self.physical_type.human_readable().to_string())
                .magenta()
                .bold(),
            Line::from(format!("{:?}", &self.stats)).magenta().bold(),
        ];

        // Render the overview
        Paragraph::new(lines).block(block).render(area, buf);
    }
}

pub fn render_column_view(area: Rect, buf: &mut Buffer, app: &mut App) {
    Block::bordered()
        .title("Column Chunk")
        .style(if app.active_pane == ActivePane::ColumnChunkDetail {
            Style::default().green()
        } else {
            Style::default().white()
        })
        .render(area, buf);
}
