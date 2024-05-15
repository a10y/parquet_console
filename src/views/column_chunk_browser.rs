use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Style, Stylize},
    text::{Line, Span},
    widgets::{Block, List, ListItem, StatefulWidget},
};

use crate::{parquet::PhysicalTypeExt, ActivePane, App};

pub fn render(area: Rect, buf: &mut Buffer, app: &mut App) {
    let chunks =
        app.parquet_metadata.row_groups[app.row_group_view_state.selected().unwrap()].columns();
    let items: Vec<ListItem> = chunks
        .iter()
        .map(|col| {
            ListItem::new(Line::from(vec![
                Span::from(col.metadata().path_in_schema.join(".")).bold(),
                Span::from("  "),
                Span::from(col.physical_type().human_readable()).magenta(),
            ]))
        })
        .collect();
    let column_chunk_list = List::new(items)
        .highlight_symbol("> ")
        .highlight_style(Style::new().bold().black().on_white())
        .block(Block::bordered().title("Column Chunks").border_style(
            if app.active_pane == ActivePane::ColumnBrowser {
                Style::default().green()
            } else {
                Style::default().white()
            },
        ));

    StatefulWidget::render(
        column_chunk_list,
        area,
        buf,
        &mut app.column_chunk_view_state,
    );
}
