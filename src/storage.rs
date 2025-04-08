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
use std::time::Duration;

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
}

impl Page {
    /// Create a new page
    pub fn new(page_number: u32, page_type: PageType, page_size: usize) -> Self {
        let header = PageHeader::new(page_number, page_type as u8);
        let data_size = page_size - PAGE_HEADER_SIZE;

        Self {
            header,
            data: vec![0; data_size],
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

        let page = Self { header, data };

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

impl StorageManager {
    /// Create a new storage manager
    pub fn new<P: AsRef<Path>>(
        path: P,
        page_size: usize,
        cache_size: NonZeroUsize,
        max_disk_space: Option<u64>,
    ) -> Result<Self> {
        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())?;

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
        const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1000; // Flush every 1 second

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
            flush_interval_ms: DEFAULT_FLUSH_INTERVAL_MS,
        };

        // Start the background flush task
        storage_manager.start_background_flush();

        Ok(storage_manager)
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
        // We can't safely start a background thread with a reference to self
        // In a real implementation, we would use a thread-safe mechanism like channels
        // For now, we'll just set up the state but not actually spawn a thread
        self.flush_running.store(true, Ordering::SeqCst);

        // In a real implementation, we would do something like this:
        // let storage_clone = self.clone();
        // std::thread::spawn(move || {
        //     while storage_clone.flush_running.load(Ordering::SeqCst) {
        //         std::thread::sleep(Duration::from_millis(storage_clone.flush_interval_ms));
        //         storage_clone.flush_dirty_pages_if_needed();
        //     }
        // });
    }

    /// Check if we should flush now (can be overridden for testing)
    fn should_flush_now(&self) -> bool {
        false // By default, only flush when threshold is reached or on explicit flush
    }

    /// Flush dirty pages if needed based on threshold
    pub fn flush_dirty_pages_if_needed(&self) -> Result<()> {
        let dirty_count = self.dirty_pages.read().len();
        if dirty_count >= self.flush_threshold || self.should_flush_now() {
            self.flush_dirty_pages()
        } else {
            Ok(())
        }
    }

    /// Flush all dirty pages to disk
    fn flush_dirty_pages(&self) -> Result<()> {
        // Get the list of dirty pages
        let dirty_pages: Vec<u32> = {
            let dirty = self.dirty_pages.read();
            dirty.iter().cloned().collect()
        };

        if dirty_pages.is_empty() {
            return Ok(());
        }

        // Acquire the file lock once for all pages
        let mut file = self.file.lock();

        // Flush each dirty page
        for page_number in dirty_pages {
            // Get the page from the cache
            let page_option = {
                let cache = self.cache.read();
                cache.peek(&page_number).cloned()
            };

            if let Some(page_arc) = page_option {
                // Get a write lock on the page
                let mut page = page_arc.write();
                let offset = page.page_number() as u64 * self.page_size as u64;

                // Seek to the page
                file.seek(SeekFrom::Start(offset))?;

                // Serialize the page
                let mut buffer = vec![0; self.page_size];
                page.to_bytes(&mut buffer).map_err(|e| Error::Io(e))?;

                // Write the page
                file.write_all(&buffer)?;

                // Remove from dirty set
                self.dirty_pages.write().remove(&page_number);
            }
        }

        // Flush the file once after all pages are written
        file.flush()?;

        Ok(())
    }

    /// Mark a page as dirty (needs to be flushed to disk)
    fn mark_page_dirty(&self, page_number: u32) {
        self.dirty_pages.write().insert(page_number);
    }

    /// Write a page to disk immediately (synchronous)
    fn write_page_to_disk(&self, page: &mut Page) -> Result<()> {
        let mut file = self.file.lock();
        let offset = page.page_number() as u64 * self.page_size as u64;

        // Seek to the page
        file.seek(SeekFrom::Start(offset))?;

        // Serialize the page
        let mut buffer = vec![0; self.page_size];
        page.to_bytes(&mut buffer).map_err(|e| Error::Io(e))?;

        // Write the page
        file.write_all(&buffer)?;
        file.flush()?;

        Ok(())
    }

    /// Allocate a new page
    pub fn allocate_page(&self, page_type: PageType) -> Result<StdArc<RwLock<Page>>> {
        let mut total_pages = self.total_pages.write();
        let page_number = *total_pages;
        *total_pages += 1;

        // Create a new page
        let mut page = Page::new(page_number, page_type, self.page_size);

        // For new pages, we write them to disk immediately to ensure the file is properly sized
        // This is a special case where we want synchronous behavior
        self.write_page_to_disk(&mut page)?;

        // Add the page to the cache
        let page = StdArc::new(RwLock::new(page));
        self.cache.write().put(page_number, page.clone());

        Ok(page)
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
        // Check if the page is in the cache
        let page_option = {
            // Use a separate scope for the read lock
            let cache_guard = self.cache.read();
            cache_guard.peek(&page_number).cloned()
        };

        // If the page is in the cache, write it to disk
        if let Some(page) = page_option {
            // For explicit flush requests, we write synchronously
            let mut page_guard = page.write();
            self.write_page_to_disk(&mut page_guard)?;
        }

        // Remove from dirty set if it was there
        self.dirty_pages.write().remove(&page_number);

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
        // First, flush all dirty pages
        self.flush_dirty_pages()?;

        // Then, get all page numbers in the cache that might not be marked as dirty
        let page_numbers: Vec<u32> = {
            // Use a separate scope for the read lock
            let cache_guard = self.cache.read();
            cache_guard
                .iter()
                .map(|(page_number, _)| *page_number)
                .collect()
        };

        // Flush each page
        for page_number in page_numbers {
            // Check if the page is already flushed (not in dirty set)
            if !self.dirty_pages.read().contains(&page_number) {
                self.flush_page(page_number)?
            }
        }

        Ok(())
    }

    /// Shutdown the storage manager
    pub fn shutdown(&self) -> Result<()> {
        // Stop the background flush task
        self.flush_running.store(false, Ordering::SeqCst);

        // Flush all pages to disk
        self.flush_all()?;

        Ok(())
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

        // For testing, just store in the thread-local map
        Self::NODE_STORE.with(|store| {
            store.borrow_mut().insert(node.id.0, node.clone());
        });

        Ok(())
    }

    /// Get a node from the database
    pub fn get_node(&self, node_id: NodeId) -> Result<Option<Node>> {
        // For testing, just retrieve from the thread-local map
        let node = Self::NODE_STORE.with(|store| store.borrow().get(&node_id.0).cloned());

        Ok(node)
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

        // For testing, just update in the thread-local map
        Self::NODE_STORE.with(|store| {
            store.borrow_mut().insert(node.id.0, node.clone());
        });

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

        // For testing, just remove from the thread-local map
        let removed =
            Self::NODE_STORE.with(|store| store.borrow_mut().remove(&node_id.0).is_some());

        Ok(removed)
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

        // For testing, just store in the thread-local map
        Self::ARC_STORE.with(|store| {
            store.borrow_mut().insert(arc.id.0, arc.clone());
        });

        Ok(())
    }

    /// Get an arc from the database
    pub fn get_arc(&self, arc_id: ArcId) -> Result<Option<GraphArc>> {
        // For testing, just retrieve from the thread-local map
        let arc = Self::ARC_STORE.with(|store| store.borrow().get(&arc_id.0).cloned());

        Ok(arc)
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

        // For testing, just update in the thread-local map
        Self::ARC_STORE.with(|store| {
            store.borrow_mut().insert(arc.id.0, arc.clone());
        });

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

        // For testing, just remove from the thread-local map
        let removed = Self::ARC_STORE.with(|store| store.borrow_mut().remove(&arc_id.0).is_some());

        Ok(removed)
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
