pub mod download;
pub mod managed;
pub mod video;
pub use download::{default_download_dir, download_url, safe_filename, DownloadProgress};
pub use managed::{download_url_managed, reserve_download_path, suggested_target, TransferControl};
pub use video::{download_hls, inspect_video, VideoVariant};
