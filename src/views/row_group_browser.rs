use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Style, Stylize},
    text::Line,
    widgets::{
        canvas::{Canvas, Rectangle},
        Block, Borders, List, StatefulWidget, Widget,
    },
};

use crate::{ActivePane, App};

pub fn render(area: Rect, buf: &mut Buffer, app: &mut App) {
    let items: Vec<String> = (0..app.num_row_groups())
        .into_iter()
        .map(|group| format!("Row Group {}", group))
        .collect();

    let row_group_list = List::new(items)
        .highlight_symbol("> ")
        .highlight_style(Style::new().bold().black().on_white())
        .block(Block::bordered().title("Row Groups").border_style(
            if app.active_pane == ActivePane::RowGroupBrowser {
                Style::default().green()
            } else {
                Style::default().white()
            },
        ));

    StatefulWidget::render(row_group_list, area, buf, &mut app.row_group_view_state);
}

pub fn render_canvas(area: Rect, buf: &mut Buffer, app: &mut App) {
    let canvas = Canvas::default()
        .x_bounds([0.0, f64::from(area.width)])
        .y_bounds([0.0, f64::from(area.height)])
        .marker(ratatui::symbols::Marker::HalfBlock)
        .block(
            Block::default()
                .title_top(Line::from(app.file_name.as_str()).centered())
                .title_bottom(
                    Line::from("   UP / DOWN to select row group    ")
                        .centered()
                        .gray(),
                )
                .borders(Borders::ALL)
                .style(if app.active_pane == ActivePane::RowGroupBrowser {
                    Style::default().green()
                } else {
                    Style::default().white()
                }),
        )
        .paint(|ctx| {
            let x_margin: f64 = 5.0;
            let y_margin: f64 = 3.0;
            let row_group_width = f64::from(area.width) - 2.0 * x_margin;
            let row_group_height = 5.0;
            for row_group in 0..app.parquet_metadata.row_groups.len() {
                let box_bottom_left_y = f64::from(area.height)
                    - y_margin
                    - row_group_height
                    - (row_group as f64) * (row_group_height + y_margin);

                ctx.print(
                    2.0,
                    box_bottom_left_y + row_group_height / 2.0,
                    format!("{}", row_group),
                );

                ctx.draw(&Rectangle {
                    x: x_margin,
                    y: box_bottom_left_y,
                    color: if row_group == app.row_group_view_state.selected().unwrap() {
                        ratatui::style::Color::Green
                    } else {
                        ratatui::style::Color::White
                    },
                    width: row_group_width,
                    height: row_group_height,
                    ..Default::default()
                })
            }
        });

    canvas.render(area, buf);
}
