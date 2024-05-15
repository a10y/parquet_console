use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Style, Stylize},
    widgets::{Block, List, StatefulWidget},
};

use crate::{ActivePane, App};

pub fn render(area: Rect, buf: &mut Buffer, app: &mut App) {
    let items: Vec<String> = (0..app.num_column_chunks())
        .into_iter()
        .map(|group| format!("Column Chunk {}", group))
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
