// Add support for ratatui

use std::io;

use clap::{Args, Parser};
use helloterm::{tui, App};

#[derive(Parser, Debug)]
enum Commands {
    Run(RunArgs),
}

#[derive(Args, Debug)]
struct RunArgs {}

fn main() -> io::Result<()> {
    let command = Commands::parse();

    // Show version of the app, based off of git tags
    match command {
        Commands::Run(_) => run_tui()?,
    }

    Ok(())
}

fn run_tui() -> io::Result<()> {
    let mut terminal = tui::init()?;

    let mut app = App::new();
    app.run(&mut terminal)?;

    // Teardown
    tui::restore()?;

    Ok(())
}
