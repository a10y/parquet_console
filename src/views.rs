use ratatui::{
    layout::{Constraint, Layout},
    Frame,
};

use crate::App;

pub mod column_chunk_browser;
pub mod column_detail;
pub mod row_group_browser;

/// Render the user interface.
pub fn render_ui(frame: &mut Frame, app: &mut App) {
    let [first_rect, second_rect, third_rect] = Layout::horizontal([
        Constraint::Percentage(33),
        Constraint::Percentage(33),
        Constraint::Percentage(33),
    ])
    .areas(frame.size());

    let buf = frame.buffer_mut();

    row_group_browser::render(first_rect, buf, app);
    column_chunk_browser::render(second_rect, buf, app);
    column_detail::render(third_rect, buf, app);
}
