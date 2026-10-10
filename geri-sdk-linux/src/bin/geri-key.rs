use std::{fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt, process::ExitCode};

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: geri-key <file>");
        return ExitCode::from(2);
    };
    match run(&path) {
        Ok(public) => {
            println!("{public}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("geri-key: {path}: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let seed = match std::fs::read_to_string(path) {
        Ok(hex) => geri_sign::parse_key(&hex).ok_or("not a 64 hex character key")?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut seed = [0; 32];
            getrandom::fill(&mut seed)?;
            let hex: String = seed.iter().map(|b| format!("{b:02x}")).collect();
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)?;
            file.write_all(format!("{hex}\n").as_bytes())?;
            file.sync_all()?;
            seed
        }
        Err(e) => return Err(e.into()),
    };
    Ok(geri_sign::public_key(&seed)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}
