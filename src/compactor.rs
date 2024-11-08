mod leveled;

use std::sync::{Arc, RwLock};

use leveled::LeveledCompactor;

use crate::config::Config;
use crate::sstable_manager::LeveledTables;

trait CompactionContext {
    fn compact(&self, level: usize, updated_tables: LeveledTables) -> Result<(), std::io::Error>;
}

pub enum Compactor {
    /// Leveled compaction
    Leveled(LeveledCompactor),
    //Tiered(TieredCompactor),
}

impl Compactor {
    pub fn leveled_compactor(config: Config, tables: Arc<RwLock<LeveledTables>>) -> Self {
        Self::Leveled(LeveledCompactor::new(config, tables))
    }
}
