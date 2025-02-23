use url::{ParseError, Url};

pub fn ensure_scheme(s: &str) -> Result<Url, ()> {
    match Url::parse(s) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            let local_path = std::path::Path::new(s).canonicalize().unwrap();
            if local_path.is_file() {
                Url::from_file_path(&local_path)
            } else {
                Url::from_directory_path(&local_path)
            }
        }
        Err(_) => Err(()),
    }
}
