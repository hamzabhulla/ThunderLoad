mod download;
mod managed;
mod video;

pub use download::{default_download_dir, download_url, reserve_download_path, safe_filename, DownloadProgress};
pub use managed::{create_transfer_control, managed_download, ManagedOutcome, TransferControl, TransferControlHandle};
pub use video::{download_hls, download_hls_managed, inspect_video, VideoVariant};
