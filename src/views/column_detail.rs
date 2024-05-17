use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    text::Line,
    widgets::{Block, Paragraph, Widget},
};

use crate::{parquet::ColumnChunkMetaDataExt, App};

pub fn render(area: Rect, buf: &mut Buffer, app: &mut App) {
    // Accept the column
    let row_group = app.row_group_view_state.selected().unwrap();
    let column = app.column_chunk_view_state.selected().unwrap();
    let chunk = app.parquet_metadata.row_groups[row_group].columns()[column].clone();

    // let phys_type = chunk.physical_type().human_readable();
    let stats = chunk.stats();

    // Add a view that centers it and displays in a pretty way
    let [_, centered_rect, _] = Layout::vertical([
        Constraint::Min(0),
        Constraint::Percentage(65),
        Constraint::Min(0),
    ])
    .areas(area);

    let lines = vec![
        Line::from(format!(
            "min = {}",
            stats.min.unwrap_or("undefined".to_string())
        )),
        Line::from(format!(
            "max = {}",
            stats.max.unwrap_or("undefined".to_string())
        )),
        Line::from(format!("nulls = {}", stats.null_count.unwrap_or(-1))),
        Line::from(format!(
            "distinct_values = {}",
            stats.distinct_values.unwrap_or(-1)
        )),
    ];

    Paragraph::new(lines)
        .block(Block::bordered().title("Column Chunk"))
        .render(centered_rect, buf);
}
