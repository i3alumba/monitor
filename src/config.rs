use anyhow::{bail, Context, Result};
pub fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}
pub fn secret(path: &str) -> Result<String> {
    credential(path, 512)
}
pub fn projected_token(path: &str) -> Result<String> {
    credential(path, 4096)
}
fn credential(path: &str, max: usize) -> Result<String> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .context("reading credential file")?
        .take((max + 2) as u64)
        .read_to_end(&mut bytes)?;
    let value = std::str::from_utf8(&bytes)?.trim();
    if value.len() < 32 || value.len() > max || !value.bytes().all(|c| c.is_ascii_graphic()) {
        bail!("invalid credential format");
    }
    Ok(value.to_owned())
}
pub fn client() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(5))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn projected_rotation_and_jwt_length() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("token");
        let path = file.to_str().context("path")?;
        std::fs::write(&file, "a".repeat(1600))?;
        assert_eq!(projected_token(path)?.len(), 1600);
        assert!(secret(path).is_err());
        std::fs::write(&file, "b".repeat(1700))?;
        assert!(projected_token(path)?.starts_with('b'));
        std::fs::write(&file, "c".repeat(4097))?;
        assert!(projected_token(path).is_err());
        Ok(())
    }
}
