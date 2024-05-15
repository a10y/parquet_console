use ratatui::{buffer::Buffer, layout::Rect};

use crate::App;

pub fn render(area: Rect, buf: &mut Buffer, app: &mut App) {
    // Render the column chunk browser as well here.
    // We also want to support canvas for scrolling off screen here instead.
    // Set the values based on where we've been scrolling through.
}
