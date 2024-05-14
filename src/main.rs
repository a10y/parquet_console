// Add support for ratatui

use std::{io, path::PathBuf};

use clap::{Args, Parser};
use color_eyre::eyre::Context;
use helloterm::{tui, App};

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

    let mut app = App::new(args.file);
    app.run(&mut terminal).wrap_err("app run failed")?;

    // Teardown
    tui::restore()?;

    Ok(())
}
