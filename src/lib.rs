use std::{fs::File, io, path::Path, time::Duration};

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use parquet2::metadata::FileMetaData;
use ratatui::{backend::Backend, widgets::ListState, Terminal};

pub mod parquet;
pub mod tui;
pub mod views;

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ActivePane {
    #[default]
    RowGroupBrowser,
    ColumnBrowser,
    ColumnChunkDetail,
}

impl ActivePane {
    pub fn toggle(&mut self) {
        *self = match self {
            ActivePane::RowGroupBrowser => ActivePane::ColumnBrowser,
            ActivePane::ColumnBrowser => ActivePane::ColumnChunkDetail,
            ActivePane::ColumnChunkDetail => ActivePane::RowGroupBrowser,
        };
    }
}

/// Wrapper around Ratatui's [ListState] with convenience handles for scrolling
/// through the list elements.
pub struct StatefulList {
    pub(crate) inner: ListState,
    pub(crate) items: Vec<String>,
}

impl StatefulList {
    pub fn from(items: Vec<String>) -> Self {
        Self {
            items,
            inner: ListState::default().with_selected(Some(0)),
        }
    }

    /// Select the menu item above the currently selected item.
    /// Wrap around back to the bottom of the list.
    pub fn up(&mut self) {
        if self.selected() == 0 {
            self.inner.select(Some(self.items.len() - 1));
        } else {
            self.inner.select(Some(self.selected() - 1));
        }
    }

    /// Select the menu item below the currently selected item.
    /// If we've reached the bottom of the list, wrap around.
    pub fn down(&mut self) {
        if self.selected() == self.items.len() - 1 {
            self.inner.select(Some(0))
        } else {
            self.inner.select(Some(self.selected() + 1))
        }
    }

    pub fn selected(&self) -> usize {
        self.inner.selected().unwrap()
    }
}

/// App is the main application, encapsulating all of the state and event-handling logic necessary to
/// drive the TUI.
pub struct App {
    pub file_name: String,
    pub file: File,
    pub parquet_metadata: FileMetaData,
    pub exiting: bool,
    pub active_pane: ActivePane,

    // Create a row group view state
    pub row_group_view_state: StatefulList,
    // pub column_chunk_view_state: StatefulList,
}

impl App {
    pub fn from<P: AsRef<Path>>(file: P) -> color_eyre::Result<Self> {
        // TODO(aduffy): OsStr is so gross
        let file_name = file
            .as_ref()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let mut file = File::open(file.as_ref()).unwrap();
        let parquet_metadata = parquet2::read::read_metadata(&mut file).unwrap();
        let row_group_items: Vec<String> = (0..parquet_metadata.row_groups.len())
            .into_iter()
            .map(|idx| format!("Row Group {}", idx))
            .collect();

        // If we have a column chunk, we want to reset it.

        Ok(Self {
            file,
            file_name,
            parquet_metadata,
            exiting: false,
            active_pane: ActivePane::default(),
            row_group_view_state: StatefulList::from(row_group_items),
            // column_chunk_view_state: StatefulList::from(),
        })
    }
}

/// Launch the TUI for Parquet file inspection.
pub fn start_ui<B: Backend>(term: &mut Terminal<B>, app: &mut App) -> color_eyre::Result<()> {
    loop {
        if app.exiting {
            return Ok(());
        }

        term.draw(|f| views::render_ui(f, app))?;

        if event::poll(Duration::from_millis(250))? {
            let evt = event::read()?;
            app.try_handle_event(evt)?;
        }
    }
}

impl App {
    pub fn try_handle_event(&mut self, event: Event) -> io::Result<()> {
        if let Event::Key(key_event) = event {
            // Only process Press events, to support Windows.
            if key_event.kind != KeyEventKind::Press {
                return Ok(());
            }

            if [KeyCode::Char('q'), KeyCode::Char('Q')].contains(&key_event.code) {
                self.exiting = true;
            }

            if key_event.code == KeyCode::Down {
                match self.active_pane {
                    ActivePane::RowGroupBrowser => {
                        self.row_group_view_state.down();
                    }
                    ActivePane::ColumnBrowser => {}
                    ActivePane::ColumnChunkDetail => {}
                }
            }

            if key_event.code == KeyCode::Up {
                match self.active_pane {
                    ActivePane::RowGroupBrowser => {
                        self.row_group_view_state.up();
                    }
                    ActivePane::ColumnBrowser => {}
                    ActivePane::ColumnChunkDetail => {}
                }
            }

            // Alternate among the selected detail modes.
            if key_event.code == KeyCode::Tab {
                self.active_pane.toggle();
            }
        }

        Ok(())
    }
}
