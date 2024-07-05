use tokio::fs::OpenOptions;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Result};
use std::mem;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct TraceEntry {
    timestamp: u32,
    obj_id: u64,
    obj_size: u32,
    next_access_vtime: i64,
}

async fn write_trace(filename: &str, entries: &[TraceEntry]) -> Result<()> {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(filename)
        .await?;
    
    for entry in entries {
        let bytes = unsafe {
            std::slice::from_raw_parts(
                entry as *const TraceEntry as *const u8,
                mem::size_of::<TraceEntry>(),
            )
        };
        file.write_all(bytes).await?;
    }
    
    Ok(())
}

async fn read_trace(filename: &str, max_entries: usize) -> Result<Vec<TraceEntry>> {
    let mut file = File::open(filename).await?;
    let mut entries = Vec::with_capacity(max_entries);
    let mut buffer = [0u8; std::mem::size_of::<TraceEntry>()];

    for _ in 0..max_entries {
        match file.read_exact(&mut buffer).await {
            Ok(_) => {
                let entry = unsafe {
                    std::mem::transmute_copy::<[u8; std::mem::size_of::<TraceEntry>()], TraceEntry>(&buffer)
                };
                entries.push(entry);
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
    }

    Ok(entries)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Example usage
    let entries = vec![
        TraceEntry { timestamp: 1234567890, obj_id: 1, obj_size: 1024, next_access_vtime: 1234567900 },
        TraceEntry { timestamp: 1234567891, obj_id: 2, obj_size: 2048, next_access_vtime: -1 },
    ];
    
    write_trace("wiki_2019t.oracleGeneral", &entries).await?;
    println!("Trace written successfully");
    
    let read_entries = read_trace("wiki_2019t.oracleGeneral", 10).await?;
    println!("Read {} entries:", read_entries.len());
    for entry in read_entries {
        println!("Timestamp: {}, Object ID: {}, Object Size: {}, Next Access VTime: {}",
                 entry.timestamp, entry.obj_id, entry.obj_size, entry.next_access_vtime);
    }
    
    Ok(())
}