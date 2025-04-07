//! Transaction log for the YarnCache database
//!
//! This module provides functionality for recording all operations in a transaction log
//! and recovering the database state from the log after a crash.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::{ArcId, Error, GraphArc, Node, NodeId, Result};

/// Types of operations that can be recorded in the transaction log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    AddNode(Node),
    UpdateNode(Node),
    DeleteNode(NodeId),
    AddArc(GraphArc),
    UpdateArc(GraphArc),
    DeleteArc(ArcId),
}

/// A transaction log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Sequence number of the entry
    pub sequence: u64,
    /// The operation
    pub operation: Operation,
    /// Checksum of the entry (calculated when writing)
    #[serde(skip)]
    pub checksum: u32,
}

/// Transaction log manager
pub struct TransactionLog {
    /// Path to the transaction log file
    log_path: PathBuf,
    /// File handle for the transaction log
    log_file: Mutex<File>,
    /// Current sequence number
    sequence: Mutex<u64>,
    /// Path to the sequence file (stores the next sequence number)
    sequence_path: PathBuf,
}

impl TransactionLog {
    /// Create a new transaction log
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let log_path = path.as_ref().to_owned();

        // Create the sequence file path by appending .seq to the log path
        let sequence_path = log_path.with_extension("log.seq");

        // Open or create the transaction log file
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&log_path)?;

        // Determine the current sequence number
        let sequence = Self::read_sequence(&sequence_path).unwrap_or_else(|_| {
            // If we can't read the sequence file, fall back to scanning the log
            let seq = Self::determine_sequence(&log_file).unwrap_or(0);
            // Write the sequence to the sequence file for next time
            let _ = Self::write_sequence(&sequence_path, seq);
            seq
        });

        Ok(Self {
            log_path,
            log_file: Mutex::new(log_file),
            sequence: Mutex::new(sequence),
            sequence_path,
        })
    }

    /// Read the sequence number from the sequence file
    fn read_sequence(path: &Path) -> Result<u64> {
        // Open the sequence file
        let mut file = File::open(path)?;

        // Read the sequence number
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;

        // Convert to u64
        let sequence = u64::from_le_bytes(buf);

        Ok(sequence)
    }

    /// Write the sequence number to the sequence file
    fn write_sequence(path: &Path, sequence: u64) -> Result<()> {
        // Create or truncate the sequence file
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // Write the sequence number
        file.write_all(&sequence.to_le_bytes())?;
        file.flush()?;

        Ok(())
    }

    /// Determine the current sequence number by reading the log
    /// This is a fallback method used when the sequence file doesn't exist
    fn determine_sequence(file: &File) -> Result<u64> {
        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            // Empty file, start at 0
            return Ok(0);
        }

        // Read the log to find the highest sequence number
        let mut reader = BufReader::new(file);
        let mut sequence = 0;

        // Seek to the beginning of the file
        reader.seek(SeekFrom::Start(0))?;

        // Read entries until we reach the end
        loop {
            match Self::read_entry(&mut reader) {
                Ok(entry) => {
                    sequence = entry.sequence;
                }
                Err(Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // End of file
                    break;
                }
                Err(e) => {
                    // Other error
                    return Err(e);
                }
            }
        }

        Ok(sequence + 1)
    }

    /// Read a log entry from the reader
    fn read_entry<R: Read>(reader: &mut R) -> Result<LogEntry> {
        // Read the length of the serialized entry
        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;

        // Read the serialized entry
        let mut entry_bytes = vec![0u8; len];
        reader.read_exact(&mut entry_bytes)?;

        // Read the checksum
        let mut checksum_bytes = [0u8; 4];
        reader.read_exact(&mut checksum_bytes)?;
        let stored_checksum = u32::from_le_bytes(checksum_bytes);

        // Calculate the checksum
        let calculated_checksum = crc32fast::hash(&entry_bytes);

        // Verify the checksum
        if calculated_checksum != stored_checksum {
            return Err(Error::Corruption(format!(
                "Transaction log entry checksum mismatch: {:x} != {:x}",
                calculated_checksum, stored_checksum
            )));
        }

        // Deserialize the entry
        let mut entry: LogEntry = bincode::deserialize(&entry_bytes)
            .map_err(|e| Error::Storage(format!("Failed to deserialize log entry: {}", e)))?;

        // Set the checksum
        entry.checksum = stored_checksum;

        Ok(entry)
    }

    /// Write a log entry to the writer
    fn write_entry<W: Write>(writer: &mut W, entry: &LogEntry) -> Result<()> {
        // Serialize the entry
        let entry_bytes = bincode::serialize(entry)
            .map_err(|e| Error::Storage(format!("Failed to serialize log entry: {}", e)))?;

        // Calculate the checksum
        let checksum = crc32fast::hash(&entry_bytes);

        // Write the length of the serialized entry
        writer.write_all(&(entry_bytes.len() as u64).to_le_bytes())?;

        // Write the serialized entry
        writer.write_all(&entry_bytes)?;

        // Write the checksum
        writer.write_all(&checksum.to_le_bytes())?;

        // Flush to ensure the entry is written to disk
        writer.flush()?;

        Ok(())
    }

    /// Append an operation to the transaction log
    pub fn append(&self, operation: Operation) -> Result<LogEntry> {
        // Create a log entry
        let sequence = {
            let mut sequence = self.sequence.lock();
            let current = *sequence;
            *sequence += 1;

            // Update the sequence file with the new next sequence number
            let _ = Self::write_sequence(&self.sequence_path, *sequence);

            current
        };

        let entry = LogEntry {
            sequence,
            operation,
            checksum: 0, // Will be calculated when writing
        };

        // Write the entry to the log
        let mut file = self.log_file.lock();

        // Seek to the end of the file
        file.seek(SeekFrom::End(0))?;

        // Write the entry
        let mut writer = BufWriter::new(&mut *file);
        Self::write_entry(&mut writer, &entry)?;

        Ok(entry)
    }

    /// Iterate over all entries in the log
    pub fn iter(&self) -> Result<LogIterator> {
        // Open the log file for reading
        let file = OpenOptions::new().read(true).open(&self.log_path)?;

        Ok(LogIterator {
            reader: BufReader::new(file),
        })
    }

    /// Truncate the log (remove all entries)
    ///
    /// This method is currently not used but is provided for future use
    /// when implementing log rotation or cleanup.
    #[allow(dead_code)]
    pub fn truncate(&self) -> Result<()> {
        let file = self.log_file.lock();

        // Truncate the file
        file.set_len(0)?;

        // Reset the sequence number
        {
            let mut sequence = self.sequence.lock();
            *sequence = 0;

            // Update the sequence file
            let _ = Self::write_sequence(&self.sequence_path, *sequence);
        }

        Ok(())
    }
}

/// Iterator over log entries
pub struct LogIterator {
    /// Reader for the log file
    reader: BufReader<File>,
}

impl LogIterator {
    /// Get the next entry from the log
    pub fn next(&mut self) -> Result<Option<LogEntry>> {
        match TransactionLog::read_entry(&mut self.reader) {
            Ok(entry) => Ok(Some(entry)),
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // End of file
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}
