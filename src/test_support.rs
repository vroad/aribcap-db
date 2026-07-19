use std::{ops::Deref, path::Path};

pub(crate) struct TestDir(tempfile::TempDir);

impl TestDir {
    pub(crate) fn new(prefix: &str) -> Self {
        Self(tempfile::Builder::new().prefix(prefix).tempdir().unwrap())
    }
}

impl Deref for TestDir {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        self.0.path()
    }
}
