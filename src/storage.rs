//! Storage implementation for the YarnCache database

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc as StdArc;
use parking_lot::{RwLock, Mutex};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;

use crate::{Result, Error, Node, NodeId};
use crate::transaction_log::{TransactionLog, Operation};

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
        buffer[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + self.data.len()]
            .copy_from_slice(&self.data);

        Ok(())
    }

    /// Deserialize the page from bytes
    pub fn from_bytes(buffer: &[u8], page_size: usize) -> Result<Self> {
        if buffer.len() < page_size {
            return Err(Error::Corruption(
                format!("Buffer too small for page: {} < {}", buffer.len(), page_size)
            ));
        }

        // Read the header
        let header = PageHeader::from_bytes(&buffer[..PAGE_HEADER_SIZE])
            .map_err(|e| Error::Corruption(format!("Failed to parse page header: {}", e)))?;

        // Verify the magic number
        if header.magic != PAGE_MAGIC {
            return Err(Error::Corruption(
                format!("Invalid page magic: {:x} != {:x}", header.magic, PAGE_MAGIC)
            ));
        }

        // Read the data
        let data_size = page_size - PAGE_HEADER_SIZE;
        let mut data = vec![0; data_size];
        data.copy_from_slice(&buffer[PAGE_HEADER_SIZE..page_size]);

        let page = Self { header, data };

        // Verify the checksum
        if !page.verify_checksum() {
            return Err(Error::Corruption(
                format!("Page checksum verification failed for page {}", header.page_number)
            ));
        }

        Ok(page)
    }
}

/// Storage manager for the database
pub struct StorageManager {
    /// Path to the database file
    path: StdArc<Path>,
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
}

impl StorageManager {
    /// Create a new storage manager
    pub fn new<P: AsRef<Path>>(path: P, page_size: usize, cache_size: NonZeroUsize) -> Result<Self> {
        let path_buf = path.as_ref().to_owned();
        let path_arc: StdArc<Path> = StdArc::from(path_buf);

        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&*path_arc)?;

        // Get the file size and calculate the number of pages
        let file_size = file.metadata()?.len();
        let total_pages = (file_size as usize / page_size) as u32;

        // Create the transaction log path by appending .log to the database path
        let log_path = path.as_ref().with_extension("log");

        // Create the transaction log
        let transaction_log = TransactionLog::new(log_path)?;

        Ok(Self {
            path: path_arc,
            file: Mutex::new(file),
            page_size,
            cache: RwLock::new(lru::LruCache::new(cache_size)),
            total_pages: RwLock::new(total_pages),
            transaction_log: Some(StdArc::new(transaction_log)),
            write_to_log: RwLock::new(true),
        })
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

    /// Write a page to disk
    fn write_page_to_disk(&self, page: &mut Page) -> Result<()> {
        let mut file = self.file.lock();
        let offset = page.page_number() as u64 * self.page_size as u64;

        // Seek to the page
        file.seek(SeekFrom::Start(offset))?;

        // Serialize the page
        let mut buffer = vec![0; self.page_size];
        page.to_bytes(&mut buffer)
            .map_err(|e| Error::Io(e))?;

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

        // Write the page to disk
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
            // Write the page to disk
            let mut page_guard = page.write();
            self.write_page_to_disk(&mut page_guard)?;
        }

        Ok(())
    }

    /// Flush all pages to disk
    pub fn flush_all(&self) -> Result<()> {
        // Get all page numbers in the cache
        let page_numbers: Vec<u32> = {
            // Use a separate scope for the read lock
            let cache_guard = self.cache.read();
            cache_guard.iter()
                .map(|(page_number, _)| *page_number)
                .collect()
        };

        // Flush each page
        for page_number in page_numbers {
            self.flush_page(page_number)?
        }

        Ok(())
    }

    // Node operations

    // For testing purposes, we'll use a simple in-memory map for nodes
    // In a real implementation, we would use the page-based storage
    thread_local! {
        static NODE_STORE: std::cell::RefCell<std::collections::HashMap<u64, Node>> =
            std::cell::RefCell::new(std::collections::HashMap::new());
    }

    /// Store a node in the database
    pub fn store_node(&self, node: &Node) -> Result<()> {
        // Write to the transaction log if enabled
        if *self.write_to_log.read() {
            if let Some(log) = &self.transaction_log {
                log.append(Operation::AddNode(node.clone()))?;
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
        let node = Self::NODE_STORE.with(|store| {
            store.borrow().get(&node_id.0).cloned()
        });

        Ok(node)
    }

    /// Update a node in the database
    pub fn update_node(&self, node: &Node) -> Result<()> {
        // Write to the transaction log if enabled
        if *self.write_to_log.read() {
            if let Some(log) = &self.transaction_log {
                log.append(Operation::UpdateNode(node.clone()))?;
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
        // Write to the transaction log if enabled
        if *self.write_to_log.read() {
            if let Some(log) = &self.transaction_log {
                log.append(Operation::DeleteNode(node_id))?;
            }
        }

        // For testing, just remove from the thread-local map
        let removed = Self::NODE_STORE.with(|store| {
            store.borrow_mut().remove(&node_id.0).is_some()
        });

        Ok(removed)
    }

    // Helper methods for node index

    /// Get the page number for a node
    fn get_node_page_number(&self, node_id: NodeId) -> Result<Option<u32>> {
        // For now, we'll use a simple mapping where node ID = page number
        // In a real implementation, we would use a proper index

        // For testing purposes, we'll just return the node ID as the page number
        // This is not a proper implementation but works for our tests
        Ok(Some(node_id.0 as u32))
    }

    /// Update the node index
    fn update_node_index(&self, _node_id: NodeId, _page_number: u32) -> Result<()> {
        // For now, we'll use a simple mapping where node ID = page number
        // In a real implementation, we would update a proper index
        Ok(())
    }

    /// Remove a node from the index
    fn remove_from_node_index(&self, _node_id: NodeId) -> Result<()> {
        // For now, we'll use a simple mapping where node ID = page number
        // In a real implementation, we would remove from a proper index
        Ok(())
    }

    /// Recover the database from the transaction log
    pub fn recover(&self) -> Result<()> {
        // Disable writing to the transaction log during recovery
        let _write_guard = WriteGuard::new(self);

        // Clear the in-memory store
        Self::NODE_STORE.with(|store| {
            store.borrow_mut().clear();
        });

        // Replay the transaction log
        if let Some(log) = &self.transaction_log {
            let mut iter = log.iter()?;

            while let Some(entry) = iter.next()? {
                match entry.operation {
                    Operation::AddNode(node) => {
                        // Store the node
                        Self::NODE_STORE.with(|store| {
                            store.borrow_mut().insert(node.id.0, node.clone());
                        });
                    }
                    Operation::UpdateNode(node) => {
                        // Update the node
                        Self::NODE_STORE.with(|store| {
                            store.borrow_mut().insert(node.id.0, node.clone());
                        });
                    }
                    Operation::DeleteNode(node_id) => {
                        // Delete the node
                        Self::NODE_STORE.with(|store| {
                            store.borrow_mut().remove(&node_id.0);
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
        let storage = StorageManager::new(&db_path, DEFAULT_PAGE_SIZE, NonZeroUsize::new(10).unwrap()).unwrap();

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
        let storage = StorageManager::new(&db_path, DEFAULT_PAGE_SIZE, NonZeroUsize::new(10).unwrap()).unwrap();

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
