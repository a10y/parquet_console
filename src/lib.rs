use std::{io, time::Duration};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::Line,
    widgets::{Block, Widget},
};

pub mod tui;

#[derive(Debug, Default)]
pub struct State {
    view_state: ViewState,
    exiting: bool,
}

#[derive(Debug, Default)]
pub enum ViewState {
    #[default]
    Initial,
}

pub struct App {
    state: State,
}

impl App {
    pub fn new() -> Self {
        Self {
            state: State {
                view_state: ViewState::Initial,
                exiting: false,
            },
        }
    }
}

impl App {
    pub fn run(&mut self, terminal: &mut tui::Tui) -> io::Result<()> {
        while !self.state.exiting {
            self.render(terminal)?;
            self.handle_events()?;
        }
        Ok(())
    }

    pub fn render(&self, terminal: &mut tui::Tui) -> io::Result<()> {
        // Render the initial view state instead.
        terminal.draw(|frame| frame.render_widget(self, frame.size()))?;
        Ok(())
    }

    pub fn handle_events(&mut self) -> io::Result<()> {
        // Non-blocking check for event presence.
        if !event::poll(Duration::from_millis(16))? {
            return Ok(());
        }

        match event::read()? {
            event::Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                if key_event.code == KeyCode::Char('q') || key_event.code == KeyCode::Char('Q') {
                    self.state.exiting = true;
                }
            }
            _ => {}
        }

        Ok(())
    }
}

// Implement the rendering logic for a render loop here.

impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Render the blocking app.
        Block::bordered()
            .title_top(Line::from("Welcome to the App").centered())
            .render(area, buf);
    }
}
