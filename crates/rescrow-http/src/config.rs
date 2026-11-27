use rescrow_application::{
    Error, api::Limits, authority::RecordKey, capabilities::DurabilityProfile,
};
use serde::{Deserialize, Serialize};
/// Transport configuration. Authority generation comes from the actual store,
/// never from a configurable serving label.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub serving_region: String,
    pub deployment_cell: String,
    pub required_profile: DurabilityProfile,
    pub cursor_key: String,
    #[serde(default)]
    pub limits: Limits,
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        self.limits.validate()?;
        RecordKey::resource(&self.serving_region)?;
        RecordKey::resource(&self.deployment_cell)?;
        super::cursor::Cursors::new(&self.cursor_key)?;
        Ok(())
    }
}
