use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    time::Duration,
};

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
}

impl ActivePane {
    pub fn toggle(&mut self) {
        *self = match self {
            ActivePane::RowGroupBrowser => ActivePane::ColumnBrowser,
            ActivePane::ColumnBrowser => ActivePane::RowGroupBrowser,
        };
    }
}

/// App is the main application, encapsulating all of the state and event-handling logic necessary to
/// drive the TUI.
pub struct App {
    pub file_name: String,
    pub path: PathBuf,
    pub parquet_metadata: FileMetaData,
    pub exiting: bool,
    pub active_pane: ActivePane,

    // Create a row group view state
    pub row_group_view_state: ListState,
    pub column_chunk_view_state: ListState,
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

        let path = file.as_ref().to_owned();
        let mut file = File::open(file.as_ref()).unwrap();
        let parquet_metadata = parquet2::read::read_metadata(&mut file).unwrap();

        Ok(Self {
            path,
            file_name,
            parquet_metadata,
            exiting: false,
            active_pane: ActivePane::default(),
            row_group_view_state: ListState::default().with_selected(Some(0)),
            column_chunk_view_state: ListState::default().with_selected(Some(0)),
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
    pub fn num_row_groups(&self) -> usize {
        self.parquet_metadata.row_groups.len()
    }

    pub fn num_column_chunks(&self) -> usize {
        self.parquet_metadata.row_groups[self.row_group_view_state.selected().unwrap()]
            .columns()
            .len()
    }

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
                        let last_selected = self.row_group_view_state.selected().unwrap();
                        if last_selected == self.num_row_groups() - 1 {
                            *self.row_group_view_state.selected_mut() = Some(0);
                        } else {
                            *self.row_group_view_state.selected_mut() = Some(last_selected + 1);
                        }

                        // Reset the column selecter
                        *self.column_chunk_view_state.selected_mut() = Some(0);
                    }
                    ActivePane::ColumnBrowser => {
                        let last_selected = self.column_chunk_view_state.selected().unwrap();
                        if last_selected == self.num_column_chunks() - 1 {
                            *self.column_chunk_view_state.selected_mut() = Some(0);
                        } else {
                            *self.column_chunk_view_state.selected_mut() = Some(last_selected + 1);
                        }
                    }
                }
            }

            if key_event.code == KeyCode::Up {
                match self.active_pane {
                    ActivePane::RowGroupBrowser => {
                        let last_selected = self.row_group_view_state.selected().unwrap();
                        if last_selected == 0 {
                            *self.row_group_view_state.selected_mut() =
                                Some(self.num_row_groups() - 1);
                        } else {
                            *self.row_group_view_state.selected_mut() = Some(last_selected - 1);
                        }

                        // Reset the column selecter
                        *self.column_chunk_view_state.selected_mut() = Some(0);
                    }
                    ActivePane::ColumnBrowser => {
                        let last_selected = self.column_chunk_view_state.selected().unwrap();
                        if last_selected == 0 {
                            *self.column_chunk_view_state.selected_mut() =
                                Some(self.num_column_chunks() - 1);
                        } else {
                            *self.column_chunk_view_state.selected_mut() = Some(last_selected - 1);
                        }
                    }
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
