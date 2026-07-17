mod db;
mod indexer;
mod ingest;
mod query;
mod record;
mod text;

#[cfg(test)]
mod test_support;

pub use db::{open_and_migrate, open_reader_pool, search_db_path};
pub use indexer::run_rebuild;
pub(crate) use indexer::{ArchiveMaintenanceConfig, run_archive_maintenance};
pub use ingest::{cleanup_index_for_deleted_files, ingest_once, ingest_paths};
pub use query::{
    CaptionLine, CaptionPage, GenreFilter, IndexedProgram, ProgramDetails, SearchFilter, SearchHit,
    SearchProgram, find_indexed_program, get_caption_page, list_indexed_programs, search_captions,
    search_combined, search_general, search_program_metadata,
};
pub use text::{
    SearchExpression, expand_from_bound, expand_to_bound, normalize_search_text,
    parse_search_expression,
};
