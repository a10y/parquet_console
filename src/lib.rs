use std::{
    cmp::min,
    fs::File,
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use crossterm::event::{self, KeyCode, KeyEventKind};
use parquet2::metadata::{FileMetaData, RowGroupMetaData};
use parquet_view::ColumnChunkMetaDataExt;
use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Layout, Rect},
    style::Stylize,
    text::Line,
    widgets::{
        canvas::{Canvas, Rectangle},
        Block, Borders, Paragraph, Widget,
    },
};

pub mod tui;

#[derive(Debug)]
pub struct State {
    view_state: ViewState,
    exiting: bool,
}

#[derive(Debug)]
pub enum ViewState {
    /// Initial state, loading from file.
    Loading(PathBuf),
    /// Display state, where everything is ready to be processed here instead.
    Displaying(String, parquet2::metadata::FileMetaData, DetailMode, usize),
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub enum DetailMode {
    #[default]
    RowGroup,
    ColumnChunk,
}

impl DetailMode {
    /// Toggle between view modes.
    pub fn toggle(self) -> Self {
        match self {
            DetailMode::RowGroup => DetailMode::ColumnChunk,
            DetailMode::ColumnChunk => DetailMode::RowGroup,
        }
    }
}

pub struct App {
    state: State,
}

impl App {
    pub fn new<P: AsRef<Path>>(file: P) -> Self {
        Self {
            state: State {
                view_state: ViewState::Loading(file.as_ref().to_owned()),
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

    pub fn render(&mut self, terminal: &mut tui::Tui) -> io::Result<()> {
        match &self.state.view_state {
            ViewState::Loading(parquet_file) => {
                terminal.draw(|frame| {
                    let widget = Block::bordered().title_top(Line::from("Loading data").centered());
                    frame.render_widget(widget, frame.size())
                })?;

                let file_name = parquet_file
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                let mut parquet_file = File::open(parquet_file)?;
                let file_metadata = parquet2::read::read_metadata(&mut parquet_file).unwrap();
                self.state.view_state = ViewState::Displaying(
                    file_name,             // Parquet file name
                    file_metadata,         // File metadata used to drive the detail views
                    DetailMode::default(), // Default detail mode for canvas rendering
                    usize::default(),      // Default selected row group
                );
            }
            ViewState::Displaying(file_name, file_metadata, detail_mode, selected_row_group) => {
                terminal.draw(|frame| {
                    let row_count = file_metadata.num_rows;
                    let row_groups = file_metadata.row_groups.len();
                    let _ = file_metadata.schema_descr;
                    let file_info = vec![
                        Line::from(format!("Row Count: {}", row_count).green().bold()),
                        Line::from(format!("Row Groups: {}", row_groups).green().bold()),
                    ];
                    let file_info_view = Paragraph::new(file_info)
                        .block(
                            Block::bordered().title_top(Line::from(file_name.as_str()).centered()),
                        )
                        .alignment(Alignment::Left);

                    // Show a split view. Include the top-level File view on the left, and allow scrolling the row groups instead here.
                    let [left, right] = Layout::horizontal([
                        Constraint::Percentage(75),
                        Constraint::Percentage(25),
                    ])
                    .areas(frame.size());

                    // Split the left panel into a small detail view, coupled with the canvas view
                    let [info, canvas] = Layout::vertical([
                        // Info view
                        Constraint::Max(10),
                        // Rest of the space
                        Constraint::Min(1),
                    ])
                    .areas(left);
                    frame.render_widget(file_info_view, info);

                    // Convert string to static
                    let file_name: &'static str = String::leak(file_name.clone());

                    // Show interactive canvas-based file browser.
                    let file_browser = ParquetBrowser::from(
                        file_name,
                        file_metadata,
                        *detail_mode,
                        *selected_row_group,
                    );
                    frame.render_widget(&file_browser, canvas);

                    // Create a new view to show the row group dtails
                    let rg_detail =
                        RowGroupDetailView::from(&file_metadata.row_groups[*selected_row_group]);
                    frame.render_widget(&rg_detail, right);
                })?;
            }
        }

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
                if key_event.code == KeyCode::Down {
                    match &mut self.state.view_state {
                        ViewState::Displaying(_, meta, _, ref mut selected) => {
                            if *selected < meta.row_groups.len() - 1 {
                                *selected += 1
                            }
                        }
                        _ => {}
                    }
                }
                if key_event.code == KeyCode::Up {
                    match &mut self.state.view_state {
                        ViewState::Displaying(_, _, _, ref mut selected) => {
                            if *selected > 0 {
                                *selected -= 1;
                            }
                        }
                        _ => {}
                    }
                }

                // Tab toggles which mode we're displaying the detail view with
                if key_event.code == KeyCode::Tab {
                    match &mut self.state.view_state {
                        ViewState::Displaying(_, _, ref mut detail_mode, _) => {
                            *detail_mode = detail_mode.toggle();
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

pub struct ParquetBrowser {
    /// Name of the Parquet file
    file_name: &'static str,
    /// Number of row groups represented in the dataset
    num_row_groups: usize,
    /// Currently selected row group value
    selected_row_group: usize,
    /// Per-RowGroup number of column chunks
    num_column_chunks: Vec<usize>,
    detail_mode: DetailMode,
}

impl ParquetBrowser {
    pub fn from(
        file_name: &'static str,
        metadata: &FileMetaData,
        detail_mode: DetailMode,
        selected_row_group: usize,
    ) -> Self {
        Self {
            file_name,
            detail_mode,
            selected_row_group,
            num_row_groups: metadata.row_groups.len(),
            num_column_chunks: metadata
                .row_groups
                .iter()
                .map(|rg| rg.columns().len())
                .collect::<Vec<_>>(),
        }
    }
}

impl Widget for &ParquetBrowser {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Determine how to split the area up
        let canvas = Canvas::default()
            .x_bounds([0.0, f64::from(area.width)])
            .y_bounds([0.0, f64::from(area.height)])
            .marker(ratatui::symbols::Marker::HalfBlock)
            .block(
                Block::default()
                    .title_top(Line::from(self.file_name).centered())
                    .title_bottom(
                        Line::from("   UP / DOWN to select row group    ")
                            .centered()
                            .gray(),
                    )
                    .borders(Borders::ALL),
            )
            .paint(|ctx| {
                let margin: f64 = 2.0;
                let row_group_width = f64::from(area.width) - 2.0 * margin;
                let row_group_height = 5.0;
                for row_group in 0..self.num_row_groups {
                    ctx.draw(&Rectangle {
                        x: margin,
                        y: f64::from(area.height)
                            - margin
                            - row_group_height
                            - (row_group as f64) * (row_group_height + margin),
                        color: if row_group == self.selected_row_group {
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
}

#[derive(Debug, Default)]
struct RowGroupDetailState {
    // Indicate the selected RowGroup for viewing their internal shit here.
    selected: usize,
}

struct RowGroupDetailView {
    metadata: RowGroupMetaData,
    state: RowGroupDetailState,
}

impl RowGroupDetailView {
    pub fn from(metadata: &RowGroupMetaData) -> Self {
        Self {
            metadata: metadata.clone(),
            state: RowGroupDetailState::default(),
        }
    }
}

impl Widget for &RowGroupDetailView {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Render as a block
        let shell = Block::bordered().title_top(Line::from("Row Group Detail").bold().green());

        let mut lines = vec![Line::from(format!(
            "Total Bytes: {}",
            self.metadata.total_byte_size()
        ))];
        lines.extend(
            self.metadata
                .columns()
                .iter()
                .map(|column_chunk_metadata| column_chunk_metadata.view())
                .flatten(),
        );

        let paragraph = Paragraph::new(lines).block(shell);

        paragraph.render(area, buf);
    }
}

/// Helper types for mapping parquet2::* types into renderable view components
mod parquet_view {
    use ratatui::{style::Stylize, text::Line};

    /// Extension trait that turns a parquet2 ColumnChunkMetadata into a list of viewable elements
    pub trait ColumnChunkMetaDataExt {
        fn view(self) -> Vec<Line<'static>>;
    }

    impl ColumnChunkMetaDataExt for &parquet2::metadata::ColumnChunkMetaData {
        fn view(self) -> Vec<Line<'static>> {
            let column_chunk = self.column_chunk().meta_data.clone().unwrap();
            let phys_typ = match self.physical_type() {
                parquet2::schema::types::PhysicalType::Boolean => "BOOLEAN",
                parquet2::schema::types::PhysicalType::Int32 => "INT32",
                parquet2::schema::types::PhysicalType::Int64 => "INT64",
                parquet2::schema::types::PhysicalType::Int96 => "INT96",
                parquet2::schema::types::PhysicalType::Float => "FLOAT",
                parquet2::schema::types::PhysicalType::Double => "DOUBLE",
                parquet2::schema::types::PhysicalType::ByteArray => "BYTEARRAY",
                parquet2::schema::types::PhysicalType::FixedLenByteArray(_) => {
                    "FIXED_LEN_BYTEARRAY"
                }
            };

            // Render a paragraph. Include the Sparklink here instead.

            vec![
                Line::from(format!("Column: {}", column_chunk.path_in_schema.join(".")))
                    .bold()
                    .green()
                    .underlined(),
                Line::from(format!("Physical Type: {}", phys_typ)),
            ]
        }
    }
}
