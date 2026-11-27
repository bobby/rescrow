//! Axum composition over the Application and bounded server boundary.
pub mod auth;
pub mod config;
mod cursor;
mod router;
pub use router::Api;
