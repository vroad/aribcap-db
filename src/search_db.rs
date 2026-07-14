mod db;
mod indexer;
mod ingest;
mod query;
mod record;
mod text;

#[cfg(test)]
mod test_support;

pub use db::{open_and_migrate, open_reader_pool, search_db_path};
pub use indexer::{ArchiveMaintenanceConfig, run_archive_maintenance, run_rebuild};
pub use ingest::{cleanup_index_for_deleted_files, ingest_once, ingest_paths};
pub use query::{
    GenreFilter, IndexedRecord, SearchFilter, SearchHit, SearchProgram, find_indexed_record,
    list_indexed_records, search_captions, search_combined, search_general, search_programs,
};
pub use text::{
    SearchExpression, expand_from_bound, expand_to_bound, normalize_search_text,
    parse_search_expression,
};
