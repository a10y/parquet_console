// Add support for ratatui

use std::io;

use helloterm::{tui, App};

fn main() -> io::Result<()> {
    let mut terminal = tui::init()?;

    let mut app = App::new();
    app.run(&mut terminal)?;

    // Teardown
    tui::restore()?;

    Ok(())
}
