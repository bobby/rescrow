use super::{Cli, Command, Error, Output, Read};
use rescrow_application::capabilities::DurabilityProfile;
use rescrow_client::{Client, Config};

pub fn run(cli: &Cli, command: Command) -> Result<Output, Error> {
    let token = if let Some(path) = &cli.token_file {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| Error::input("could not read token file"))?
            .take(16 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::input("could not read token file"))?;
        if bytes.len() > 16 * 1024 {
            return Err(Error::input("token file exceeds 16 KiB"));
        }
        String::from_utf8(bytes)
            .map_err(|_| Error::input("token file must be UTF-8"))?
            .trim_end_matches(['\r', '\n'])
            .to_owned()
    } else {
        std::env::var("RESCROW_TOKEN")
            .map_err(|_| Error::input("set RESCROW_TOKEN or --token-file for remote mode"))?
    };
    let mut config = Config::new(cli.endpoint[0].clone(), token);
    config.endpoints = cli.endpoint.clone();
    config.authority_generation = cli.authority_generation.clone();
    config.required_profile = match cli.require_profile.as_deref().unwrap_or("simulation") {
        "local" => DurabilityProfile::Local,
        "regional" => DurabilityProfile::Regional,
        "enterprise-mr" => DurabilityProfile::EnterpriseMr,
        _ => DurabilityProfile::Simulation,
    };
    let client = Client::new(config)?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| Error::input("could not start remote client runtime"))?
        .block_on(client.execute(command))
}
