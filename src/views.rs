use ratatui::{
    layout::{Constraint, Layout},
    Frame,
};

use crate::App;

use self::{column_chunk::render_column_view, row_group_browser::render};

pub mod column_chunk;
pub mod column_chunk_browser;
pub mod row_group_browser;

/// Render the user interface.
pub fn render_ui(frame: &mut Frame, app: &mut App) {
    let [left_rect, right_rect] =
        Layout::horizontal([Constraint::Percentage(60), Constraint::Min(0)]).areas(frame.size());

    let [top_right, bottom_right] =
        Layout::vertical([Constraint::Percentage(70), Constraint::Min(1)]).areas(right_rect);

    let buf = frame.buffer_mut();

    row_group_browser::render(left_rect, buf, app);
    render_column_view(bottom_right, buf, app);
}
