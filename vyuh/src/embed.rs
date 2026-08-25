use rust_silos::{File as SiloFile, SiloSet};
use std::io::Read;
use std::path::Path;

/// Macro-expansion support for [`embed_assets!`].
///
/// Applications should use [`embed_assets!`] instead of these implementation types.
#[doc(hidden)]
pub use rust_silos::{EmbedEntry, Silo};

/// Embeds one asset directory through Vyuh's asset facade.
pub use vyuh_macros::embed_assets;

/// Wrapper around rust-silos File with sync/async read methods
pub struct File {
    inner: SiloFile,
}

impl File {
    fn new(inner: SiloFile) -> Self {
        Self { inner }
    }

    pub fn base_name(&self) -> Option<&str> {
        self.inner.path().file_name()?.to_str()
    }

    pub fn is_embedded(&self) -> bool {
        self.inner.is_embedded()
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn read_bytes_sync(&self) -> std::io::Result<Vec<u8>> {
        let mut reader = self
            .inner
            .reader()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(buf)
    }

    pub async fn read_bytes_async(&self) -> std::io::Result<Vec<u8>> {
        self.read_bytes_sync()
    }
}

/// Wrapper around rust-silos Silo
#[derive(Clone, Debug)]
pub struct Dir {
    inner: Silo,
}

impl Dir {
    pub fn empty() -> Self {
        Self {
            inner: Silo::new(""),
        }
    }

    pub const fn new(silo: Silo) -> Self {
        Self { inner: silo }
    }

    pub fn is_embedded(&self) -> bool {
        self.inner.is_embedded()
    }

    pub fn path(&self) -> &Path {
        Path::new("")
    }

    pub fn get_file(&self, name: &str) -> Option<File> {
        self.inner.get_file(name).map(File::new)
    }

    pub(crate) fn into_silo(self) -> Silo {
        self.inner
    }
}

impl From<Silo> for Dir {
    fn from(silo: Silo) -> Self {
        Self { inner: silo }
    }
}

/// Collection of directories with overlay support
pub struct DirSet {
    inner: SiloSet,
}

impl DirSet {
    pub fn new(dirs: Vec<Dir>) -> Self {
        let silos: Vec<Silo> = dirs.into_iter().map(|d| d.inner).collect();
        Self {
            inner: SiloSet::new(silos),
        }
    }

    pub fn get_file(&self, name: &str) -> Option<File> {
        self.inner.get_file(name).map(File::new)
    }

    pub fn walk(&self) -> impl Iterator<Item = File> {
        let files: Vec<File> = self.inner.iter().map(File::new).collect();
        files.into_iter()
    }

    /// Walks files using later directory entries as path-level overrides.
    pub fn walk_override(&self) -> impl Iterator<Item = File> {
        let files: Vec<File> = self.inner.iter_override().map(File::new).collect();
        files.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use std::{io, path::Path};

    use super::*;

    fn asset_dir(path: &Path) -> Result<Dir, io::Error> {
        let root = path
            .to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 asset path"))?;
        Ok(Dir::new(Silo::new(root)))
    }

    /// Verifies that later asset directories override matching relative paths.
    #[test]
    fn walk_override_uses_later_file() -> Result<(), io::Error> {
        let first = tempfile::tempdir()?;
        let second = tempfile::tempdir()?;
        std::fs::write(first.path().join("shared.txt"), "first")?;
        std::fs::write(second.path().join("shared.txt"), "second")?;

        let files = DirSet::new(vec![asset_dir(first.path())?, asset_dir(second.path())?])
            .walk_override()
            .collect::<Vec<_>>();

        assert_eq!(files.len(), 1);
        let bytes = files
            .first()
            .ok_or_else(|| io::Error::other("overlaid file was not found"))?
            .read_bytes_sync()?;
        assert_eq!(bytes, b"second");
        Ok(())
    }
}
