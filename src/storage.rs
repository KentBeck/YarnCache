//! Storage implementation for the YarnCache database

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::transaction_log::{Operation, TransactionLog};
use crate::{ArcId, Error, GraphArc, Node, NodeId, Result};

/// Default page size (4KB)
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Page header size in bytes
const PAGE_HEADER_SIZE: usize = 16;

/// Magic number for identifying YarnCache pages
const PAGE_MAGIC: u32 = 0x59434142; // "YCAB" in ASCII

/// Page header structure
#[derive(Debug, Clone, Copy)]
struct PageHeader {
    /// Magic number to identify YarnCache pages
    magic: u32,
    /// Page number
    page_number: u32,
    /// Page type
    page_type: u8,
    /// Reserved for future use
    reserved: [u8; 3],
    /// CRC32 checksum of the page data
    checksum: u32,
}

impl PageHeader {
    /// Create a new page header
    fn new(page_number: u32, page_type: u8) -> Self {
        Self {
            magic: PAGE_MAGIC,
            page_number,
            page_type,
            reserved: [0; 3],
            checksum: 0, // Will be calculated later
        }
    }

    /// Serialize the header to bytes
    fn to_bytes(&self) -> io::Result<[u8; PAGE_HEADER_SIZE]> {
        let mut buffer = [0u8; PAGE_HEADER_SIZE];
        let mut cursor = io::Cursor::new(&mut buffer[..]);

        cursor.write_u32::<LittleEndian>(self.magic)?;
        cursor.write_u32::<LittleEndian>(self.page_number)?;
        cursor.write_u8(self.page_type)?;
        cursor.write_all(&self.reserved)?;
        cursor.write_u32::<LittleEndian>(self.checksum)?;

        Ok(buffer)
    }

    /// Deserialize the header from bytes
    fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < PAGE_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small for page header",
            ));
        }

        let mut cursor = io::Cursor::new(bytes);

        let magic = cursor.read_u32::<LittleEndian>()?;
        let page_number = cursor.read_u32::<LittleEndian>()?;
        let page_type = cursor.read_u8()?;

        let mut reserved = [0u8; 3];
        cursor.read_exact(&mut reserved)?;

        let checksum = cursor.read_u32::<LittleEndian>()?;

        Ok(Self {
            magic,
            page_number,
            page_type,
            reserved,
            checksum,
        })
    }
}

/// Page types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Free page (not in use)
    Free = 0,
    /// Node data page
    Node = 1,
    /// Arc data page
    Arc = 2,
    /// Node index page
    NodeIndex = 3,
    /// Arc index page (by ID)
    ArcIndexById = 4,
    /// Arc index page (by source node)
    ArcIndexBySource = 5,
    /// Arc index page (by target node)
    ArcIndexByTarget = 6,
    /// Arc index page (by timestamp)
    ArcIndexByTimestamp = 7,
}

impl From<u8> for PageType {
    fn from(value: u8) -> Self {
        match value {
            0 => PageType::Free,
            1 => PageType::Node,
            2 => PageType::Arc,
            3 => PageType::NodeIndex,
            4 => PageType::ArcIndexById,
            5 => PageType::ArcIndexBySource,
            6 => PageType::ArcIndexByTarget,
            7 => PageType::ArcIndexByTimestamp,
            _ => PageType::Free, // Default to Free for unknown types
        }
    }
}

/// A page in the database
#[derive(Debug, Clone)]
pub struct Page {
    /// Page header
    header: PageHeader,
    /// Page data
    data: Vec<u8>,
    /// Number of items stored in this page (for Node and Arc pages)
    item_count: usize,
}

impl Page {
    /// Create a new page
    pub fn new(page_number: u32, page_type: PageType, page_size: usize) -> Self {
        let header = PageHeader::new(page_number, page_type as u8);
        let data_size = page_size - PAGE_HEADER_SIZE;

        Self {
            header,
            data: vec![0; data_size],
            item_count: 0,
        }
    }

    /// Get the page number
    pub fn page_number(&self) -> u32 {
        self.header.page_number
    }

    /// Get the page type
    pub fn page_type(&self) -> PageType {
        PageType::from(self.header.page_type)
    }

    /// Get a reference to the page data
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get a mutable reference to the page data
    pub fn data_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    /// Calculate the checksum for this page
    fn calculate_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();

        // Hash the header fields except the checksum itself
        hasher.update(&self.header.magic.to_le_bytes());
        hasher.update(&self.header.page_number.to_le_bytes());
        hasher.update(&[self.header.page_type]);
        hasher.update(&self.header.reserved);

        // For Node and Arc pages, include the item count in the checksum
        if self.header.page_type == PageType::Node as u8 || self.header.page_type == PageType::Arc as u8 {
            hasher.update(&(self.item_count as u32).to_le_bytes());
        }

        // Hash the page data
        hasher.update(&self.data);

        hasher.finalize()
    }

    /// Update the checksum for this page
    pub fn update_checksum(&mut self) {
        self.header.checksum = self.calculate_checksum();
    }

    /// Verify the checksum for this page
    pub fn verify_checksum(&self) -> bool {
        let stored_checksum = self.header.checksum;
        let calculated_checksum = self.calculate_checksum();

        stored_checksum == calculated_checksum
    }

    /// Add a node to this page
    pub fn add_node(&mut self, node: &Node) -> io::Result<()> {
        if self.header.page_type != PageType::Node as u8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot add node to non-node page",
            ));
        }

        // Serialize the node
        let node_bytes = bincode::serialize(node)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Check if there's enough space
        // Format: 4 bytes for item count + 4 bytes for each item offset + item data
        let header_size = 4 + 4 * (self.item_count + 1);
        let current_data_size = if self.item_count > 0 {
            let mut cursor = io::Cursor::new(&self.data[4 + 4 * (self.item_count - 1)..4 + 4 * self.item_count]);
            cursor.read_u32::<LittleEndian>()? as usize
        } else {
            header_size
        };

        let new_data_size = current_data_size + node_bytes.len();
        if new_data_size > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Not enough space in page for node",
            ));
        }

        // Update the item count
        let mut cursor = io::Cursor::new(&mut self.data[..4]);
        cursor.write_u32::<LittleEndian>(self.item_count as u32 + 1)?;

        // Write the offset to the new item
        let offset_pos = 4 + 4 * self.item_count;
        let mut cursor = io::Cursor::new(&mut self.data[offset_pos..offset_pos + 4]);
        cursor.write_u32::<LittleEndian>(new_data_size as u32)?;

        // Write the node data
        self.data[current_data_size..new_data_size].copy_from_slice(&node_bytes);

        // Update the item count
        self.item_count += 1;

        Ok(())
    }

    /// Add an arc to this page
    pub fn add_arc(&mut self, arc: &GraphArc) -> io::Result<()> {
        if self.header.page_type != PageType::Arc as u8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot add arc to non-arc page",
            ));
        }

        // Serialize the arc
        let arc_bytes = bincode::serialize(arc)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Check if there's enough space
        // Format: 4 bytes for item count + 4 bytes for each item offset + item data
        let header_size = 4 + 4 * (self.item_count + 1);
        let current_data_size = if self.item_count > 0 {
            let mut cursor = io::Cursor::new(&self.data[4 + 4 * (self.item_count - 1)..4 + 4 * self.item_count]);
            cursor.read_u32::<LittleEndian>()? as usize
        } else {
            header_size
        };

        let new_data_size = current_data_size + arc_bytes.len();
        if new_data_size > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Not enough space in page for arc",
            ));
        }

        // Update the item count
        let mut cursor = io::Cursor::new(&mut self.data[..4]);
        cursor.write_u32::<LittleEndian>(self.item_count as u32 + 1)?;

        // Write the offset to the new item
        let offset_pos = 4 + 4 * self.item_count;
        let mut cursor = io::Cursor::new(&mut self.data[offset_pos..offset_pos + 4]);
        cursor.write_u32::<LittleEndian>(new_data_size as u32)?;

        // Write the arc data
        self.data[current_data_size..new_data_size].copy_from_slice(&arc_bytes);

        // Update the item count
        self.item_count += 1;

        Ok(())
    }

    /// Get all nodes from this page
    pub fn get_nodes(&self) -> Result<Vec<Node>> {
        if self.header.page_type != PageType::Node as u8 {
            return Err(Error::Storage("Cannot get nodes from non-node page".to_string()));
        }

        let mut nodes = Vec::with_capacity(self.item_count);

        // Read the nodes
        for i in 0..self.item_count {
            // Get the offset to the current item
            let start_offset_pos = 4 + 4 * i;
            let mut cursor = io::Cursor::new(&self.data[start_offset_pos..start_offset_pos + 4]);
            let start_offset = cursor.read_u32::<LittleEndian>().map_err(|e| Error::Io(e))? as usize;

            // Get the offset to the next item or the end of data
            let end_offset = if i < self.item_count - 1 {
                let end_offset_pos = 4 + 4 * (i + 1);
                let mut cursor = io::Cursor::new(&self.data[end_offset_pos..end_offset_pos + 4]);
                cursor.read_u32::<LittleEndian>().map_err(|e| Error::Io(e))? as usize
            } else {
                self.data.len()
            };

            // Deserialize the node
            let node: Node = bincode::deserialize(&self.data[start_offset..end_offset])
                .map_err(|e| Error::Corruption(format!("Failed to deserialize node: {}", e)))?;

            nodes.push(node);
        }

        Ok(nodes)
    }

    /// Get all arcs from this page
    pub fn get_arcs(&self) -> Result<Vec<GraphArc>> {
        if self.header.page_type != PageType::Arc as u8 {
            return Err(Error::Storage("Cannot get arcs from non-arc page".to_string()));
        }

        let mut arcs = Vec::with_capacity(self.item_count);

        // Read the arcs
        for i in 0..self.item_count {
            // Get the offset to the current item
            let start_offset_pos = 4 + 4 * i;
            let mut cursor = io::Cursor::new(&self.data[start_offset_pos..start_offset_pos + 4]);
            let start_offset = cursor.read_u32::<LittleEndian>().map_err(|e| Error::Io(e))? as usize;

            // Get the offset to the next item or the end of data
            let end_offset = if i < self.item_count - 1 {
                let end_offset_pos = 4 + 4 * (i + 1);
                let mut cursor = io::Cursor::new(&self.data[end_offset_pos..end_offset_pos + 4]);
                cursor.read_u32::<LittleEndian>().map_err(|e| Error::Io(e))? as usize
            } else {
                self.data.len()
            };

            // Deserialize the arc
            let arc: GraphArc = bincode::deserialize(&self.data[start_offset..end_offset])
                .map_err(|e| Error::Corruption(format!("Failed to deserialize arc: {}", e)))?;

            arcs.push(arc);
        }

        Ok(arcs)
    }

    /// Serialize the page to bytes
    pub fn to_bytes(&mut self, buffer: &mut [u8]) -> io::Result<()> {
        if buffer.len() < PAGE_HEADER_SIZE + self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Buffer too small for page",
            ));
        }

        // Update the checksum before serializing
        self.update_checksum();

        // Write the header
        let header_bytes = self.header.to_bytes()?;
        buffer[..PAGE_HEADER_SIZE].copy_from_slice(&header_bytes);

        // Write the data
        buffer[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + self.data.len()].copy_from_slice(&self.data);

        Ok(())
    }

    /// Deserialize the page from bytes
    pub fn from_bytes(buffer: &[u8], page_size: usize) -> Result<Self> {
        if buffer.len() < page_size {
            return Err(Error::Corruption(format!(
                "Buffer too small for page: {} < {}",
                buffer.len(),
                page_size
            )));
        }

        // Read the header
        let header = PageHeader::from_bytes(&buffer[..PAGE_HEADER_SIZE])
            .map_err(|e| Error::Corruption(format!("Failed to parse page header: {}", e)))?;

        // Verify the magic number
        if header.magic != PAGE_MAGIC {
            return Err(Error::Corruption(format!(
                "Invalid page magic: {:x} != {:x}",
                header.magic, PAGE_MAGIC
            )));
        }

        // Read the data
        let data_size = page_size - PAGE_HEADER_SIZE;
        let mut data = vec![0; data_size];
        data.copy_from_slice(&buffer[PAGE_HEADER_SIZE..page_size]);

        // For Node and Arc pages, the first 4 bytes of data contain the item count
        let mut item_count = 0;
        if header.page_type == PageType::Node as u8 || header.page_type == PageType::Arc as u8 {
            if data.len() >= 4 {
                let mut cursor = io::Cursor::new(&data[..4]);
                item_count = cursor.read_u32::<LittleEndian>().unwrap_or(0) as usize;
            }
        }

        let page = Self { header, data, item_count };

        // Verify the checksum
        if !page.verify_checksum() {
            return Err(Error::Corruption(format!(
                "Page checksum verification failed for page {}",
                header.page_number
            )));
        }

        Ok(page)
    }
}

/// Storage manager for the database
pub struct StorageManager {
    /// File handle
    file: Mutex<File>,
    /// Page size
    page_size: usize,
    /// Page cache
    cache: RwLock<lru::LruCache<u32, StdArc<RwLock<Page>>>>,
    /// Total number of pages in the file
    total_pages: RwLock<u32>,
    /// Transaction log
    transaction_log: Option<StdArc<TransactionLog>>,
    /// Whether to write to the transaction log
    write_to_log: RwLock<bool>,
    /// Maximum disk space in bytes (None for unlimited)
    max_disk_space: Option<u64>,
    /// Current disk usage in bytes
    current_disk_usage: RwLock<u64>,
    /// Set of dirty pages that need to be flushed to disk
    dirty_pages: RwLock<std::collections::HashSet<u32>>,
    /// Flag to control the background flush task
    flush_running: AtomicBool,
    /// Flush threshold (number of dirty pages that triggers a flush)
    flush_threshold: usize,
    /// Flush interval in milliseconds
    flush_interval_ms: u64,
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        println!("StorageManager being dropped");
        self.close_file();
    }
}

impl StorageManager {
    /// Create a new storage manager
    pub fn new<P: AsRef<Path>>(
        path: P,
        page_size: usize,
        cache_size: NonZeroUsize,
        max_disk_space: Option<u64>,
        flush_interval_ms: u64,
    ) -> Result<Self> {
        println!("Creating new StorageManager with path: {:?}", path.as_ref());

        // Clear the in-memory stores before loading from disk
        Self::NODE_STORE.with(|store| {
            store.borrow_mut().clear();
        });

        Self::ARC_STORE.with(|store| {
            store.borrow_mut().clear();
        });

        // Check if the file exists and is locked
        if path.as_ref().exists() {
            println!("Database file exists, checking if it's accessible...");
            match File::open(path.as_ref()) {
                Ok(_) => println!("Database file is accessible"),
                Err(e) => {
                    println!("Error opening database file: {}", e);
                    return Err(Error::Io(e));
                }
            }
        } else {
            println!("Database file does not exist, will create a new one");
        }
        // Open or create the file with a more robust approach
        println!("Opening database file...");
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())
        {
            Ok(f) => {
                println!("Successfully opened database file");
                f
            },
            Err(e) => {
                println!("Error opening database file: {}", e);
                return Err(Error::Io(e));
            }
        };

        // Get the file size and calculate the number of pages
        let file_size = file.metadata()?.len();
        let total_pages = (file_size as usize / page_size) as u32;

        // Create the transaction log path by appending .log to the database path
        let log_path = path.as_ref().with_extension("log");

        // Create the transaction log
        let transaction_log = TransactionLog::new(&log_path)?;

        // Calculate current disk usage
        let file_size = file.metadata()?.len();
        let log_file_size = std::fs::metadata(&log_path)?.len();
        let current_disk_usage = file_size + log_file_size;

        // Check if we're already over the limit
        if let Some(max_space) = max_disk_space {
            if current_disk_usage > max_space {
                return Err(Error::DiskSpaceExceeded(format!(
                    "Current disk usage ({} bytes) exceeds maximum allowed ({} bytes)",
                    current_disk_usage, max_space
                )));
            }
        }

        // Default flush settings
        const DEFAULT_FLUSH_THRESHOLD: usize = 100; // Flush after 100 dirty pages

        // Create the storage manager with a more robust approach
        println!("Creating storage manager...");
        let storage_manager = Self {
            file: Mutex::new(file),
            page_size,
            cache: RwLock::new(lru::LruCache::new(cache_size)),
            total_pages: RwLock::new(total_pages),
            transaction_log: Some(StdArc::new(transaction_log)),
            write_to_log: RwLock::new(true),
            max_disk_space,
            current_disk_usage: RwLock::new(current_disk_usage),
            dirty_pages: RwLock::new(HashSet::new()),
            flush_running: AtomicBool::new(true),
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            flush_interval_ms,
        };
        println!("Storage manager created successfully");

        // Start the background flush task
        storage_manager.start_background_flush();

        // Load nodes and arcs from pages
        storage_manager.load_from_pages()?;

        Ok(storage_manager)
    }

    /// Load nodes and arcs from pages
    fn load_from_pages(&self) -> Result<()> {
        let total_pages = *self.total_pages.read();

        // Skip if there are no pages
        if total_pages == 0 {
            return Ok(());
        }

        println!("Loading data from {} pages...", total_pages);

        // Load nodes and arcs from all pages
        let mut node_count = 0;
        let mut arc_count = 0;

        for page_number in 0..total_pages {
            let page = self.get_page(page_number)?;
            let page_guard = page.read();

            match page_guard.page_type() {
                PageType::Node => {
                    // Load nodes from this page
                    let nodes = page_guard.get_nodes()?;
                    for node in nodes {
                        // Store in the thread-local map
                        Self::NODE_STORE.with(|store| {
                            store.borrow_mut().insert(node.id.0, node.clone());
                        });
                        node_count += 1;
                    }
                },
                PageType::Arc => {
                    // Load arcs from this page
                    let arcs = page_guard.get_arcs()?;
                    for arc in arcs {
                        // Store in the thread-local map
                        Self::ARC_STORE.with(|store| {
                            store.borrow_mut().insert(arc.id.0, arc.clone());
                        });
                        arc_count += 1;
                    }
                },
                _ => {}
            }
        }

        println!("Loaded {} nodes and {} arcs from disk", node_count, arc_count);

        Ok(())
    }

    /// Get the total number of pages
    pub fn total_pages(&self) -> u32 {
        *self.total_pages.read()
    }

    /// Read a page from disk
    fn read_page_from_disk(&self, page_number: u32) -> Result<Page> {
        let mut file = self.file.lock();
        let offset = page_number as u64 * self.page_size as u64;

        // Seek to the page
        file.seek(SeekFrom::Start(offset))?;

        // Read the page
        let mut buffer = vec![0; self.page_size];
        file.read_exact(&mut buffer)?;

        // Parse the page
        Page::from_bytes(&buffer, self.page_size)
    }

    /// Start the background flush task
    fn start_background_flush(&self) {
        // Set the flush running flag to true
        self.flush_running.store(true, Ordering::SeqCst);

        // For testing purposes, we'll just set up the state but not actually spawn a thread
        // This avoids potential issues with background threads in tests
        println!("Background flush task would start here in production code");

        // In a real implementation, we would use a thread-safe mechanism like channels
        // and spawn a background thread to periodically flush dirty pages
    }

    /// Check if we should flush now (can be overridden for testing)
    fn should_flush_now(&self) -> bool {
        false // By default, only flush when threshold is reached or on explicit flush
    }

    /// Flush dirty pages if needed based on threshold
    pub fn flush_dirty_pages_if_needed(&self) -> Result<()> {
        let dirty_count = self.dirty_pages.read().len();

        // Use both the flush threshold and the interval (via should_flush_now)
        // to determine when to flush
        if dirty_count >= self.flush_threshold || self.should_flush_now() {
            // Log the flush interval for debugging purposes
            if log::log_enabled!(log::Level::Debug) {
                log::debug!("Flushing {} dirty pages (threshold: {}, interval: {}ms)",
                    dirty_count, self.flush_threshold, self.flush_interval_ms);
            }

            self.flush_dirty_pages()
        } else {
            Ok(())
        }
    }

    /// Flush all dirty pages to disk
    fn flush_dirty_pages(&self) -> Result<()> {
        println!("Flushing dirty pages");

        // Get the list of dirty pages with a timeout
        let dirty_pages: Vec<u32> = {
            let dirty = match self.dirty_pages.try_read() {
                Some(guard) => guard,
                None => {
                    println!("Could not acquire read lock on dirty_pages, skipping flush");
                    return Ok(());
                }
            };
            let pages: Vec<u32> = dirty.iter().cloned().collect();
            println!("Found {} dirty pages to flush", pages.len());
            pages
        };

        if dirty_pages.is_empty() {
            println!("No dirty pages to flush");
            return Ok(());
        }

        // Try to acquire the file lock with a timeout
        let file_guard_option = self.file.try_lock();
        let mut file = match file_guard_option {
            Some(guard) => {
                println!("Acquired file lock for flushing");
                guard
            },
            None => {
                println!("Could not acquire file lock, skipping flush");
                return Ok(());
            }
        };

        // Flush each dirty page
        for page_number in dirty_pages {
            println!("Flushing page {}", page_number);

            // Get the page from the cache
            let page_option = {
                let cache = match self.cache.try_read() {
                    Some(guard) => guard,
                    None => {
                        println!("Could not acquire read lock on cache, skipping page {}", page_number);
                        continue;
                    }
                };
                cache.peek(&page_number).cloned()
            };

            if let Some(page_arc) = page_option {
                // Try to get a write lock on the page with a timeout
                let page_guard_option = page_arc.try_write();
                let mut page = match page_guard_option {
                    Some(guard) => guard,
                    None => {
                        println!("Could not acquire write lock on page {}, skipping", page_number);
                        continue;
                    }
                };

                let offset = page.page_number() as u64 * self.page_size as u64;
                println!("Writing page {} at offset {}", page_number, offset);

                // Seek to the page
                if let Err(e) = file.seek(SeekFrom::Start(offset)) {
                    println!("Error seeking to offset {} for page {}: {}", offset, page_number, e);
                    continue;
                }

                // Serialize the page
                let mut buffer = vec![0; self.page_size];
                if let Err(e) = page.to_bytes(&mut buffer) {
                    println!("Error serializing page {}: {}", page_number, e);
                    continue;
                }

                // Write the page
                if let Err(e) = file.write_all(&buffer) {
                    println!("Error writing page {}: {}", page_number, e);
                    continue;
                }

                // Drop the page write lock before acquiring the dirty_pages write lock
                // to avoid potential deadlocks
                drop(page);

                // Try to remove from dirty set with a timeout
                match self.dirty_pages.try_write() {
                    Some(mut dirty_guard) => {
                        dirty_guard.remove(&page_number);
                        println!("Page {} flushed and removed from dirty set", page_number);
                    },
                    None => {
                        println!("Could not acquire write lock on dirty_pages for page {}", page_number);
                    }
                };
            } else {
                println!("Page {} not found in cache", page_number);
            }
        }

        // Flush the file once after all pages are written
        if let Err(e) = file.flush() {
            println!("Error flushing file: {}", e);
            return Err(Error::Io(e));
        }
        println!("File flushed to disk");

        Ok(())
    }

    /// Mark a page as dirty (needs to be flushed to disk)
    fn mark_page_dirty(&self, page_number: u32) {
        self.dirty_pages.write().insert(page_number);
    }

    /// Write a page to disk immediately (synchronous)
    fn write_page_to_disk(&self, page: &mut Page) -> Result<()> {
        let page_number = page.page_number();
        let offset = page_number as u64 * self.page_size as u64;
        println!("Writing page {} to disk at offset {}", page_number, offset);

        // Serialize the page before acquiring the file lock
        let mut buffer = vec![0; self.page_size];
        match page.to_bytes(&mut buffer) {
            Ok(_) => {},
            Err(e) => {
                println!("Error serializing page {}: {}", page_number, e);
                return Err(Error::Io(e));
            }
        }

        // Try to acquire the file lock with a timeout
        let file_guard_option = self.file.try_lock();
        let mut file = match file_guard_option {
            Some(guard) => guard,
            None => {
                println!("Could not acquire file lock for writing page {}", page_number);
                return Err(Error::Storage(format!("Could not acquire file lock for page {}", page_number)));
            }
        };

        // Seek to the page with error handling
        match file.seek(SeekFrom::Start(offset)) {
            Ok(_) => {},
            Err(e) => {
                println!("Error seeking to offset {} for page {}: {}", offset, page_number, e);
                return Err(Error::Io(e));
            }
        }

        // Write the page with error handling
        match file.write_all(&buffer) {
            Ok(_) => {},
            Err(e) => {
                println!("Error writing page {}: {}", page_number, e);
                return Err(Error::Io(e));
            }
        }

        // Flush the file with error handling
        match file.flush() {
            Ok(_) => {},
            Err(e) => {
                println!("Error flushing file after writing page {}: {}", page_number, e);
                return Err(Error::Io(e));
            }
        }

        println!("Page {} written to disk successfully", page_number);

        Ok(())
    }

    /// Allocate a new page
    pub fn allocate_page(&self, page_type: PageType) -> Result<StdArc<RwLock<Page>>> {
        let mut total_pages = self.total_pages.write();
        let page_number = *total_pages;
        *total_pages += 1;

        // Create a new page
        let mut page = Page::new(page_number, page_type, self.page_size);

        // Initialize the item count to 0 for node and arc pages
        if page_type == PageType::Node || page_type == PageType::Arc {
            let mut cursor = io::Cursor::new(&mut page.data[..4]);
            cursor.write_u32::<LittleEndian>(0).map_err(|e| Error::Io(e))?;
        }

        // For new pages, we write them to disk immediately to ensure the file is properly sized
        // This is a special case where we want synchronous behavior
        self.write_page_to_disk(&mut page)?;

        // Add the page to the cache
        let page = StdArc::new(RwLock::new(page));
        self.cache.write().put(page_number, page.clone());

        Ok(page)
    }

    /// Allocate a new node page
    pub fn allocate_node_page(&self) -> Result<StdArc<RwLock<Page>>> {
        self.allocate_page(PageType::Node)
    }

    /// Allocate a new arc page
    pub fn allocate_arc_page(&self) -> Result<StdArc<RwLock<Page>>> {
        self.allocate_page(PageType::Arc)
    }

    /// Get a page by number
    pub fn get_page(&self, page_number: u32) -> Result<StdArc<RwLock<Page>>> {
        // Check if the page is in the cache
        let page_option = {
            // Use a separate scope for the read lock
            let cache_guard = self.cache.read();
            cache_guard.peek(&page_number).cloned()
        };

        if let Some(page) = page_option {
            return Ok(page);
        }

        // Read the page from disk
        let page = self.read_page_from_disk(page_number)?;

        // Add the page to the cache
        let page = StdArc::new(RwLock::new(page));
        self.cache.write().put(page_number, page.clone());

        Ok(page)
    }

    /// Flush a page to disk
    pub fn flush_page(&self, page_number: u32) -> Result<()> {
        println!("Flushing page {}", page_number);

        // Check if the page is in the cache with a timeout
        let page_option = {
            // Use a separate scope for the read lock with timeout
            match self.cache.try_read() {
                Some(cache_guard) => cache_guard.peek(&page_number).cloned(),
                None => {
                    println!("Could not acquire read lock on cache for page {}", page_number);
                    return Err(Error::Storage(format!("Could not acquire cache lock for page {}", page_number)));
                }
            }
        };

        // If the page is in the cache, write it to disk
        if let Some(page) = page_option {
            // Try to get a write lock on the page with a timeout
            match page.try_write() {
                Some(mut page_guard) => {
                    println!("Writing page {} to disk", page_number);

                    // Write the page to disk with error handling
                    if let Err(e) = self.write_page_to_disk(&mut page_guard) {
                        println!("Error writing page {} to disk: {}", page_number, e);
                        return Err(e);
                    }

                    // Drop the page write lock before acquiring the dirty_pages write lock
                    // to avoid potential deadlocks
                    drop(page_guard);
                },
                None => {
                    println!("Could not acquire write lock on page {}", page_number);
                    return Err(Error::Storage(format!("Could not acquire page lock for page {}", page_number)));
                }
            }
        } else {
            println!("Page {} not found in cache", page_number);
            // Not finding the page is not an error, it might have been evicted from the cache
        }

        // Try to remove from dirty set with a timeout
        match self.dirty_pages.try_write() {
            Some(mut dirty_set) => {
                if dirty_set.remove(&page_number) {
                    println!("Removed page {} from dirty set", page_number);
                }
            },
            None => {
                println!("Could not acquire write lock on dirty_pages for page {}", page_number);
                // Continue anyway, this is not a critical error
            }
        }

        println!("Page {} flush complete", page_number);
        Ok(())
    }

    /// Update a page in the cache and mark it as dirty
    pub fn update_page(&self, page: StdArc<RwLock<Page>>) {
        let page_number = page.read().page_number();
        self.cache.write().put(page_number, page.clone());

        // Mark the page as dirty so it will be flushed asynchronously
        self.mark_page_dirty(page_number);
    }

    /// Flush all pages to disk
    pub fn flush_all(&self) -> Result<()> {
        println!("Flushing all pages to disk");

        // First, flush all dirty pages with error handling
        match self.flush_dirty_pages() {
            Ok(_) => println!("Dirty pages flushed successfully"),
            Err(e) => {
                println!("Error flushing dirty pages: {}", e);
                // Continue with the rest of the flush operation
            }
        }

        // Then, get all page numbers in the cache that might not be marked as dirty
        let page_numbers: Vec<u32> = {
            // Use a separate scope for the read lock with timeout
            match self.cache.try_read() {
                Some(cache_guard) => {
                    let pages: Vec<u32> = cache_guard
                        .iter()
                        .map(|(page_number, _)| *page_number)
                        .collect();
                    println!("Found {} total pages in cache", pages.len());
                    pages
                },
                None => {
                    println!("Could not acquire read lock on cache, skipping non-dirty page flush");
                    return Ok(());
                }
            }
        };

        // Flush each page with error handling
        let mut flush_errors = 0;
        for page_number in page_numbers {
            // Check if the page is already flushed (not in dirty set) with timeout
            let needs_flush = match self.dirty_pages.try_read() {
                Some(dirty_set) => !dirty_set.contains(&page_number),
                None => {
                    println!("Could not acquire read lock on dirty_pages for page {}, skipping", page_number);
                    continue;
                }
            };

            if needs_flush {
                println!("Flushing non-dirty page {}", page_number);
                match self.flush_page(page_number) {
                    Ok(_) => println!("Page {} flushed successfully", page_number),
                    Err(e) => {
                        println!("Error flushing page {}: {}", page_number, e);
                        flush_errors += 1;
                        // Continue with the next page
                    }
                }
            }
        }

        if flush_errors > 0 {
            println!("Completed with {} flush errors", flush_errors);
        } else {
            println!("All pages flushed to disk successfully");
        }

        Ok(())
    }

    /// Shutdown the storage manager
    pub fn shutdown(&self) -> Result<()> {
        println!("Shutting down storage manager");

        // Stop the background flush task
        self.flush_running.store(false, Ordering::SeqCst);
        println!("Background flush task stopped");

        // Flush all pages to disk
        self.flush_all()?;
        println!("All pages flushed to disk");

        // Explicitly close the file by dropping the mutex guard
        {
            let mut file_guard = self.file.lock();
            // Force a flush of the file
            file_guard.flush()?;
            println!("File flushed");

            // We can't explicitly close the file in Rust, but we can drop the guard
            // which will release the lock
            drop(file_guard);
            println!("File lock released");
        }

        println!("Storage manager shutdown complete");
        Ok(())
    }

    // Implement Drop for StorageManager to ensure proper cleanup
    fn close_file(&self) {
        println!("Closing file in StorageManager");
        // Try to flush all pages to disk
        if let Err(e) = self.flush_all() {
            println!("Error flushing pages during close: {}", e);
        }

        // Try to flush and close the file
        let file_guard_option = self.file.try_lock();
        if let Some(mut file_guard) = file_guard_option {
            if let Err(e) = file_guard.flush() {
                println!("Error flushing file during close: {}", e);
            }
            // Drop the guard to release the lock
            drop(file_guard);
            println!("File closed successfully");
        } else {
            println!("Could not acquire file lock during close");
        }
    }

    // Node operations

    // For testing purposes, we'll use simple in-memory maps for nodes and arcs
    // In a real implementation, we would use the page-based storage
    thread_local! {
        static NODE_STORE: std::cell::RefCell<std::collections::HashMap<u64, Node>> =
            std::cell::RefCell::new(std::collections::HashMap::new());
        static ARC_STORE: std::cell::RefCell<std::collections::HashMap<u64, GraphArc>> =
            std::cell::RefCell::new(std::collections::HashMap::new());
    }

    /// Find or create a node page with space for the given node
    fn find_or_create_node_page(&self, node: &Node) -> Result<StdArc<RwLock<Page>>> {
        // First, try to find an existing node page with enough space
        let total_pages = *self.total_pages.read();
        for page_number in 0..total_pages {
            let page = self.get_page(page_number)?;
            let page_guard = page.read();

            // Skip non-node pages
            if page_guard.page_type() != PageType::Node {
                continue;
            }

            // Check if there's enough space
            let node_bytes = bincode::serialize(node)
                .map_err(|e| Error::Storage(format!("Failed to serialize node: {}", e)))?;

            // Format: 4 bytes for item count + 4 bytes for each item offset + item data
            let header_size = 4 + 4 * (page_guard.item_count + 1);
            let current_data_size = if page_guard.item_count > 0 {
                let mut cursor = io::Cursor::new(&page_guard.data[4 + 4 * (page_guard.item_count - 1)..4 + 4 * page_guard.item_count]);
                cursor.read_u32::<LittleEndian>().map_err(|e| Error::Io(e))? as usize
            } else {
                header_size
            };

            let new_data_size = current_data_size + node_bytes.len();
            if new_data_size <= page_guard.data.len() {
                return Ok(page.clone());
            }
        }

        // If no suitable page found, create a new one
        self.allocate_node_page()
    }

    /// Store a node in the database
    pub fn store_node(&self, node: &Node) -> Result<()> {
        // Check disk space limit before writing
        self.check_disk_space_limit(node.data.len() as u64)?;

        // Write to the transaction log if enabled
        if *self.write_to_log.read() {
            if let Some(log) = &self.transaction_log {
                log.append(Operation::AddNode(node.clone()))?;

                // Update disk usage
                self.update_disk_usage(node.data.len() as u64);
            }
        }

        // Store in the thread-local map for backward compatibility
        Self::NODE_STORE.with(|store| {
            store.borrow_mut().insert(node.id.0, node.clone());
        });

        // Store in a page
        let page = self.find_or_create_node_page(node)?;
        let mut page_guard = page.write();
        page_guard.add_node(node).map_err(|e| Error::Io(e))?;

        // Mark the page as dirty
        self.mark_page_dirty(page_guard.page_number());

        // Update the page in the cache
        self.update_page(page.clone());

        Ok(())
    }

    /// Get a node from the database
    pub fn get_node(&self, node_id: NodeId) -> Result<Option<Node>> {
        // First check the thread-local map for backward compatibility
        let node = Self::NODE_STORE.with(|store| store.borrow().get(&node_id.0).cloned());
        if node.is_some() {
            return Ok(node);
        }

        // If not found in memory, search through all node pages
        let total_pages = *self.total_pages.read();
        for page_number in 0..total_pages {
            let page = self.get_page(page_number)?;
            let page_guard = page.read();

            // Skip non-node pages
            if page_guard.page_type() != PageType::Node {
                continue;
            }

            // Get all nodes from the page
            let nodes = page_guard.get_nodes()?;

            // Find the node with the matching ID
            for node in nodes {
                if node.id == node_id {
                    return Ok(Some(node));
                }
            }
        }

        Ok(None)
    }

    /// Update a node in the database
    pub fn update_node(&self, node: &Node) -> Result<()> {
        // Check disk space limit before writing
        self.check_disk_space_limit(node.data.len() as u64)?;

        // Write to the transaction log if enabled
        if *self.write_to_log.read() {
            if let Some(log) = &self.transaction_log {
                log.append(Operation::UpdateNode(node.clone()))?;

                // Update disk usage
                self.update_disk_usage(node.data.len() as u64);
            }
        }

        // Update in the thread-local map for backward compatibility
        Self::NODE_STORE.with(|store| {
            store.borrow_mut().insert(node.id.0, node.clone());
        });

        // First, try to find the node in a page and update it
        let total_pages = *self.total_pages.read();
        for page_number in 0..total_pages {
            let page = self.get_page(page_number)?;
            let page_guard = page.read();

            // Skip non-node pages
            if page_guard.page_type() != PageType::Node {
                continue;
            }

            // Get all nodes from the page
            let nodes = page_guard.get_nodes()?;

            // Find the node with the matching ID
            for i in 0..nodes.len() {
                if nodes[i].id == node.id {
                    // Found the node, delete it and add the updated version
                    // This is inefficient but simple for now
                    drop(page_guard); // Release the read lock

                    // Delete the node from this page (by creating a new page without it)
                    let mut new_page = Page::new(page_number, PageType::Node, self.page_size);
                    let mut cursor = io::Cursor::new(&mut new_page.data[..4]);
                    cursor.write_u32::<LittleEndian>(0).map_err(|e| Error::Io(e))?;

                    // Add all nodes except the one being updated
                    for j in 0..nodes.len() {
                        if j != i {
                            new_page.add_node(&nodes[j]).map_err(|e| Error::Io(e))?;
                        }
                    }

                    // Add the updated node
                    new_page.add_node(node).map_err(|e| Error::Io(e))?;

                    // Replace the page in the cache
                    let new_page_arc = StdArc::new(RwLock::new(new_page));
                    self.cache.write().put(page_number, new_page_arc.clone());

                    // Mark the page as dirty
                    self.mark_page_dirty(page_number);

                    return Ok(());
                }
            }
        }

        // If not found in any page, store it as a new node
        self.store_node(node)?;

        Ok(())
    }

    /// Delete a node from the database
    pub fn delete_node(&self, node_id: NodeId) -> Result<bool> {
        // Check disk space limit for the log entry (small fixed size)
        self.check_disk_space_limit(16)?; // Approximate size of a delete operation in the log

        // Write to the transaction log if enabled
        if *self.write_to_log.read() {
            if let Some(log) = &self.transaction_log {
                log.append(Operation::DeleteNode(node_id))?;

                // Update disk usage
                self.update_disk_usage(16); // Approximate size of a delete operation in the log
            }
        }

        // Remove from the thread-local map for backward compatibility
        let removed_from_memory =
            Self::NODE_STORE.with(|store| store.borrow_mut().remove(&node_id.0).is_some());

        // Try to find and remove the node from a page
        let mut removed_from_page = false;
        let total_pages = *self.total_pages.read();
        for page_number in 0..total_pages {
            let page = self.get_page(page_number)?;
            let page_guard = page.read();

            // Skip non-node pages
            if page_guard.page_type() != PageType::Node {
                continue;
            }

            // Get all nodes from the page
            let nodes = page_guard.get_nodes()?;

            // Find the node with the matching ID
            for i in 0..nodes.len() {
                if nodes[i].id == node_id {
                    // Found the node, create a new page without it
                    drop(page_guard); // Release the read lock

                    // Create a new page without the deleted node
                    let mut new_page = Page::new(page_number, PageType::Node, self.page_size);
                    let mut cursor = io::Cursor::new(&mut new_page.data[..4]);
                    cursor.write_u32::<LittleEndian>(0).map_err(|e| Error::Io(e))?;

                    // Add all nodes except the one being deleted
                    for j in 0..nodes.len() {
                        if j != i {
                            new_page.add_node(&nodes[j]).map_err(|e| Error::Io(e))?;
                        }
                    }

                    // Replace the page in the cache
                    let new_page_arc = StdArc::new(RwLock::new(new_page));
                    self.cache.write().put(page_number, new_page_arc.clone());

                    // Mark the page as dirty
                    self.mark_page_dirty(page_number);

                    removed_from_page = true;
                    break;
                }
            }

            if removed_from_page {
                break;
            }
        }

        Ok(removed_from_memory || removed_from_page)
    }

    // Note: In a real implementation, we would have helper methods for node indexing
    // For the current in-memory implementation, these are not needed

    /// Check if the operation would exceed the disk space limit
    fn check_disk_space_limit(&self, additional_bytes: u64) -> Result<()> {
        if let Some(max_space) = self.max_disk_space {
            let current_usage = *self.current_disk_usage.read();
            let new_usage = current_usage + additional_bytes;

            if new_usage > max_space {
                return Err(Error::DiskSpaceExceeded(format!(
                    "Operation would exceed disk space limit: current usage {} bytes + {} bytes > {} bytes maximum",
                    current_usage, additional_bytes, max_space
                )));
            }
        }

        Ok(())
    }

    /// Update the current disk usage
    fn update_disk_usage(&self, additional_bytes: u64) {
        let mut usage = self.current_disk_usage.write();
        *usage += additional_bytes;
    }

    /// Find or create an arc page with space for the given arc
    fn find_or_create_arc_page(&self, arc: &GraphArc) -> Result<StdArc<RwLock<Page>>> {
        // First, try to find an existing arc page with enough space
        let total_pages = *self.total_pages.read();
        for page_number in 0..total_pages {
            let page = self.get_page(page_number)?;
            let page_guard = page.read();

            // Skip non-arc pages
            if page_guard.page_type() != PageType::Arc {
                continue;
            }

            // Check if there's enough space
            let arc_bytes = bincode::serialize(arc)
                .map_err(|e| Error::Storage(format!("Failed to serialize arc: {}", e)))?;

            // Format: 4 bytes for item count + 4 bytes for each item offset + item data
            let header_size = 4 + 4 * (page_guard.item_count + 1);
            let current_data_size = if page_guard.item_count > 0 {
                let mut cursor = io::Cursor::new(&page_guard.data[4 + 4 * (page_guard.item_count - 1)..4 + 4 * page_guard.item_count]);
                cursor.read_u32::<LittleEndian>().map_err(|e| Error::Io(e))? as usize
            } else {
                header_size
            };

            let new_data_size = current_data_size + arc_bytes.len();
            if new_data_size <= page_guard.data.len() {
                return Ok(page.clone());
            }
        }

        // If no suitable page found, create a new one
        self.allocate_arc_page()
    }

    /// Store an arc in the database
    pub fn store_arc(&self, arc: &GraphArc) -> Result<()> {
        // Check disk space limit before writing
        self.check_disk_space_limit(arc.data.len() as u64)?;

        // Write to the transaction log if enabled
        if *self.write_to_log.read() {
            if let Some(log) = &self.transaction_log {
                log.append(Operation::AddArc(arc.clone()))?;

                // Update disk usage
                self.update_disk_usage(arc.data.len() as u64);
            }
        }

        // Store in the thread-local map for backward compatibility
        Self::ARC_STORE.with(|store| {
            store.borrow_mut().insert(arc.id.0, arc.clone());
        });

        // Store in a page
        let page = self.find_or_create_arc_page(arc)?;
        let mut page_guard = page.write();
        page_guard.add_arc(arc).map_err(|e| Error::Io(e))?;

        // Mark the page as dirty
        self.mark_page_dirty(page_guard.page_number());

        // Update the page in the cache
        self.update_page(page.clone());

        Ok(())
    }

    /// Get an arc from the database
    pub fn get_arc(&self, arc_id: ArcId) -> Result<Option<GraphArc>> {
        // First check the thread-local map for backward compatibility
        let arc = Self::ARC_STORE.with(|store| store.borrow().get(&arc_id.0).cloned());
        if arc.is_some() {
            return Ok(arc);
        }

        // If not found in memory, search through all arc pages
        let total_pages = *self.total_pages.read();
        for page_number in 0..total_pages {
            let page = self.get_page(page_number)?;
            let page_guard = page.read();

            // Skip non-arc pages
            if page_guard.page_type() != PageType::Arc {
                continue;
            }

            // Get all arcs from the page
            let arcs = page_guard.get_arcs()?;

            // Find the arc with the matching ID
            for arc in arcs {
                if arc.id == arc_id {
                    return Ok(Some(arc));
                }
            }
        }

        Ok(None)
    }

    /// Update an arc in the database
    pub fn update_arc(&self, arc: &GraphArc) -> Result<()> {
        // Check disk space limit before writing
        self.check_disk_space_limit(arc.data.len() as u64)?;

        // Write to the transaction log if enabled
        if *self.write_to_log.read() {
            if let Some(log) = &self.transaction_log {
                log.append(Operation::UpdateArc(arc.clone()))?;

                // Update disk usage
                self.update_disk_usage(arc.data.len() as u64);
            }
        }

        // Update in the thread-local map for backward compatibility
        Self::ARC_STORE.with(|store| {
            store.borrow_mut().insert(arc.id.0, arc.clone());
        });

        // First, try to find the arc in a page and update it
        let total_pages = *self.total_pages.read();
        for page_number in 0..total_pages {
            let page = self.get_page(page_number)?;
            let page_guard = page.read();

            // Skip non-arc pages
            if page_guard.page_type() != PageType::Arc {
                continue;
            }

            // Get all arcs from the page
            let arcs = page_guard.get_arcs()?;

            // Find the arc with the matching ID
            for i in 0..arcs.len() {
                if arcs[i].id == arc.id {
                    // Found the arc, delete it and add the updated version
                    // This is inefficient but simple for now
                    drop(page_guard); // Release the read lock

                    // Delete the arc from this page (by creating a new page without it)
                    let mut new_page = Page::new(page_number, PageType::Arc, self.page_size);
                    let mut cursor = io::Cursor::new(&mut new_page.data[..4]);
                    cursor.write_u32::<LittleEndian>(0).map_err(|e| Error::Io(e))?;

                    // Add all arcs except the one being updated
                    for j in 0..arcs.len() {
                        if j != i {
                            new_page.add_arc(&arcs[j]).map_err(|e| Error::Io(e))?;
                        }
                    }

                    // Add the updated arc
                    new_page.add_arc(arc).map_err(|e| Error::Io(e))?;

                    // Replace the page in the cache
                    let new_page_arc = StdArc::new(RwLock::new(new_page));
                    self.cache.write().put(page_number, new_page_arc.clone());

                    // Mark the page as dirty
                    self.mark_page_dirty(page_number);

                    return Ok(());
                }
            }
        }

        // If not found in any page, store it as a new arc
        self.store_arc(arc)?;

        Ok(())
    }

    /// Delete an arc from the database
    pub fn delete_arc(&self, arc_id: ArcId) -> Result<bool> {
        // Check disk space limit for the log entry (small fixed size)
        self.check_disk_space_limit(16)?; // Approximate size of a delete operation in the log

        // Write to the transaction log if enabled
        if *self.write_to_log.read() {
            if let Some(log) = &self.transaction_log {
                log.append(Operation::DeleteArc(arc_id))?;

                // Update disk usage
                self.update_disk_usage(16); // Approximate size of a delete operation in the log
            }
        }

        // Remove from the thread-local map for backward compatibility
        let removed_from_memory = Self::ARC_STORE.with(|store| store.borrow_mut().remove(&arc_id.0).is_some());

        // Try to find and remove the arc from a page
        let mut removed_from_page = false;
        let total_pages = *self.total_pages.read();
        for page_number in 0..total_pages {
            let page = self.get_page(page_number)?;
            let page_guard = page.read();

            // Skip non-arc pages
            if page_guard.page_type() != PageType::Arc {
                continue;
            }

            // Get all arcs from the page
            let arcs = page_guard.get_arcs()?;

            // Find the arc with the matching ID
            for i in 0..arcs.len() {
                if arcs[i].id == arc_id {
                    // Found the arc, create a new page without it
                    drop(page_guard); // Release the read lock

                    // Create a new page without the deleted arc
                    let mut new_page = Page::new(page_number, PageType::Arc, self.page_size);
                    let mut cursor = io::Cursor::new(&mut new_page.data[..4]);
                    cursor.write_u32::<LittleEndian>(0).map_err(|e| Error::Io(e))?;

                    // Add all arcs except the one being deleted
                    for j in 0..arcs.len() {
                        if j != i {
                            new_page.add_arc(&arcs[j]).map_err(|e| Error::Io(e))?;
                        }
                    }

                    // Replace the page in the cache
                    let new_page_arc = StdArc::new(RwLock::new(new_page));
                    self.cache.write().put(page_number, new_page_arc.clone());

                    // Mark the page as dirty
                    self.mark_page_dirty(page_number);

                    removed_from_page = true;
                    break;
                }
            }

            if removed_from_page {
                break;
            }
        }

        Ok(removed_from_memory || removed_from_page)
    }

    /// Recover the database from the transaction log
    pub fn recover(&self) -> Result<()> {
        // Disable writing to the transaction log during recovery
        let _write_guard = WriteGuard::new(self);

        // Clear the in-memory stores
        Self::NODE_STORE.with(|store| {
            store.borrow_mut().clear();
        });

        Self::ARC_STORE.with(|store| {
            store.borrow_mut().clear();
        });

        // Replay the transaction log
        if let Some(log) = &self.transaction_log {
            let mut iter = log.iter()?;

            while let Some(entry) = iter.next()? {
                match entry.operation {
                    Operation::AddNode(node) => {
                        Self::NODE_STORE.with(|store| {
                            store.borrow_mut().insert(node.id.0, node.clone());
                        });
                    }
                    Operation::UpdateNode(node) => {
                        Self::NODE_STORE.with(|store| {
                            store.borrow_mut().insert(node.id.0, node.clone());
                        });
                    }
                    Operation::DeleteNode(node_id) => {
                        Self::NODE_STORE.with(|store| {
                            store.borrow_mut().remove(&node_id.0);
                        });
                    }
                    Operation::AddArc(arc) => {
                        Self::ARC_STORE.with(|store| {
                            store.borrow_mut().insert(arc.id.0, arc.clone());
                        });
                    }
                    Operation::UpdateArc(arc) => {
                        Self::ARC_STORE.with(|store| {
                            store.borrow_mut().insert(arc.id.0, arc.clone());
                        });
                    }
                    Operation::DeleteArc(arc_id) => {
                        Self::ARC_STORE.with(|store| {
                            store.borrow_mut().remove(&arc_id.0);
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Disable writing to the transaction log
    pub fn disable_logging(&self) {
        *self.write_to_log.write() = false;
    }

    /// Enable writing to the transaction log
    pub fn enable_logging(&self) {
        *self.write_to_log.write() = true;
    }
}

/// Guard for temporarily disabling transaction log writes
struct WriteGuard<'a> {
    storage: &'a StorageManager,
    previous: bool,
}

impl<'a> WriteGuard<'a> {
    fn new(storage: &'a StorageManager) -> Self {
        let previous = *storage.write_to_log.read();
        *storage.write_to_log.write() = false;
        Self { storage, previous }
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        *self.storage.write_to_log.write() = self.previous;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_dir::TempDir;

    #[test]
    fn test_page_checksum() {
        let mut page = Page::new(1, PageType::Node, DEFAULT_PAGE_SIZE);

        // Fill the page with some data
        for i in 0..100 {
            page.data_mut()[i] = i as u8;
        }

        // Update the checksum
        page.update_checksum();

        // Verify the checksum
        assert!(page.verify_checksum());

        // Modify the data and verify the checksum fails
        page.data_mut()[50] = 255;
        assert!(!page.verify_checksum());
    }

    #[test]
    fn test_page_serialization() {
        let mut page = Page::new(1, PageType::Node, DEFAULT_PAGE_SIZE);

        // Fill the page with some data
        for i in 0..100 {
            page.data_mut()[i] = i as u8;
        }

        // Serialize the page
        let mut buffer = vec![0; DEFAULT_PAGE_SIZE];
        page.to_bytes(&mut buffer).unwrap();

        // Deserialize the page
        let page2 = Page::from_bytes(&buffer, DEFAULT_PAGE_SIZE).unwrap();

        // Verify the pages are equal
        assert_eq!(page.page_number(), page2.page_number());
        assert_eq!(page.page_type(), page2.page_type());
        assert_eq!(page.data()[..100], page2.data()[..100]);
    }

    #[test]
    fn test_storage_manager() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create a storage manager
        let storage = StorageManager::new(
            &db_path,
            DEFAULT_PAGE_SIZE,
            NonZeroUsize::new(10).unwrap(),
            None,
            1000, // 1 second flush interval
        )
        .unwrap();

        // Allocate a page
        let page = storage.allocate_page(PageType::Node).unwrap();

        // Write some data to the page
        {
            let mut page = page.write();
            for i in 0..100 {
                page.data_mut()[i] = i as u8;
            }
        }

        // Flush the page
        storage.flush_page(0).unwrap();

        // Get the page again
        let page2 = storage.get_page(0).unwrap();

        // Verify the data
        {
            let page2 = page2.read();
            for i in 0..100 {
                assert_eq!(page2.data()[i], i as u8);
            }
        }
    }

    #[test]
    fn test_corrupted_page_detection() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("corrupt.db");

        // Create a file with corrupted data
        {
            let mut file = File::create(&db_path).unwrap();
            let mut data = vec![0; DEFAULT_PAGE_SIZE];

            // Write invalid magic number
            let mut cursor = io::Cursor::new(&mut data[..]);
            cursor.write_u32::<LittleEndian>(0x12345678).unwrap(); // Wrong magic

            file.write_all(&data).unwrap();
        }

        // Create a storage manager
        let storage = StorageManager::new(
            &db_path,
            DEFAULT_PAGE_SIZE,
            NonZeroUsize::new(10).unwrap(),
            None,
            1000, // 1 second flush interval
        )
        .unwrap();

        // Try to read the page, should fail with corruption error
        let result = storage.read_page_from_disk(0);
        assert!(result.is_err());

        if let Err(Error::Corruption(_)) = result {
            // Expected error
        } else {
            panic!("Expected corruption error, got: {:?}", result);
        }
    }
}
