// Add support for ratatui

use std::path::PathBuf;

use clap::{Args, Parser};
use color_eyre::eyre::Context;
use parquet_console::{start_ui, tui, App};

#[derive(Parser, Debug)]
enum Commands {
    Inspect(InspectArgs),
}

#[derive(Args, Debug)]
struct InspectArgs {
    #[arg(value_name = "FILE")]
    pub file: PathBuf,
}

fn main() -> color_eyre::Result<()> {
    let command = Commands::parse();

    // Show version of the app, based off of git
    match command {
        Commands::Inspect(args) => run_tui(args).wrap_err("run tui failed")?,
    }

    Ok(())
}

/// Run TUI application for inspecting Parquet files
fn run_tui(args: InspectArgs) -> color_eyre::Result<()> {
    tui::install_hooks()?;
    let mut terminal = tui::init().wrap_err("tui::init failed")?;

    let mut app = App::from(args.file)?;
    start_ui(&mut terminal, &mut app)?;

    // Teardown
    tui::restore()?;

    Ok(())
}
