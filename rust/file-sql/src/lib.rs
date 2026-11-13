// Copyright (c) 2026 OceanBase. Licensed under Apache License 2.0.
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use parquet::basic::{ConvertedType, Repetition, Type as Physical};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::record::{reader::RowIter, Field};
use parquet::schema::types::Type;
use parquet::thrift::TSerializable;
use parquet::format::PageHeader;
use thrift::protocol::TCompactInputProtocol;

#[cxx::bridge(namespace = "oceanbase::sql::parquet_io")]
mod ffi {
    struct Column {
        name: String,
        // 1 boolean, 2 signed integer, 3 float, 4 date, 5 timestamp, 6 UTF-8/binary
        kind: u8,
        nullable: bool,
    }
    struct Cell {
        null: bool,
        kind: u8,
        integer: i64,
        floating: f64,
        bytes: Vec<u8>,
    }
    struct Row { eof: bool, cells: Vec<Cell>, error: String }
    extern "Rust" {
        type ParquetScan;
        // The descriptor is duplicated; ownership of the C++ descriptor stays with C++.
        fn open_descriptor(fd: i32, projection: &[u32]) -> Result<Box<ParquetScan>>;
        fn row_count(self: &ParquetScan) -> u64;
        fn open_checked(fd: i32, projection: &[u32]) -> Box<ParquetScan>;
        fn error(self: &ParquetScan) -> &str;
        fn read_checked(self: &mut ParquetScan) -> Row;
        fn columns(self: &ParquetScan) -> Vec<Column>;
        fn next_row(self: &mut ParquetScan) -> Result<Row>;
    }
}

pub struct ParquetScan {
    iterator: Option<RowIter<'static>>,
    error: String,
    schema: Vec<ffi::Column>,
    row_count: u64,
}

#[cfg(unix)]
fn duplicate_file(fd: i32) -> std::io::Result<File> {
    use std::os::fd::{BorrowedFd, FromRawFd, IntoRawFd};
    // The caller must keep fd valid for the duration of this synchronous call.
    if fd < 0 { return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,"invalid file descriptor")); }
    let owned = unsafe { BorrowedFd::borrow_raw(fd) }.try_clone_to_owned()?;
    Ok(unsafe { File::from_raw_fd(owned.into_raw_fd()) })
}
#[cfg(not(unix))]
fn duplicate_file(_: i32) -> std::io::Result<File> {
    Err(std::io::Error::new(std::io::ErrorKind::Unsupported,"local Parquet descriptor requires Unix"))
}
// Validate compact Thrift declarations without allocating from untrusted lengths.
// Apply before upstream metadata/page decoding, which reserves declared lengths.
fn compact_preflight(input: &mut impl Read) -> Result<(), String> {
    fn byte(r: &mut impl Read) -> Result<u8,String> {
        let mut b=[0]; r.read_exact(&mut b).map_err(|e| e.to_string())?; Ok(b[0])
    }
    fn varint(r: &mut impl Read) -> Result<u64,String> {
        let mut v=0u64;
        for shift in (0..70).step_by(7) {
            let b=byte(r)?;
            if shift==63 && b>1 { return Err("invalid compact varint".into()); }
            v |= u64::from(b & 127) << shift;
            if b & 128 == 0 { return Ok(v); }
        }
        Err("invalid compact varint".into())
    }
    fn skip(r: &mut impl Read, n: u64) -> Result<(),String> {
        if n>16*1024*1024 { return Err("compact byte length exceeds limit".into()); }
        let mut left=n; let mut buf=[0u8;4096];
        while left>0 {
            let size=left.min(buf.len() as u64) as usize;
            r.read_exact(&mut buf[..size]).map_err(|e| e.to_string())?; left-=size as u64;
        }
        Ok(())
    }
    fn value(r: &mut impl Read, kind: u8, depth: u32, budget: &mut u64, field: bool) -> Result<(),String> {
        if depth>32 || *budget==0 { return Err("compact nesting/item limit exceeded".into()); }
        *budget-=1;
        match kind {
            1|2 => { if !field { let b=byte(r)?; if b!=1 && b!=2 { return Err("invalid compact boolean".into()); } } }
            3 => { byte(r)?; }
            4|5|6 => { varint(r)?; }
            7 => skip(r,8)?,
            8 => { let n=varint(r)?; skip(r,n)?; }
            9|10 => {
                let h=byte(r)?; let mut n=u64::from(h>>4);
                if n==15 { n=varint(r)?; }
                if n>100_000 || n>*budget { return Err("compact collection exceeds limit".into()); }
                for _ in 0..n { value(r,h&15,depth+1,budget,false)?; }
            }
            11 => {
                let n=varint(r)?;
                if n>100_000 || n.saturating_mul(2)>*budget { return Err("compact map exceeds limit".into()); }
                if n>0 { let h=byte(r)?; for _ in 0..n { value(r,h>>4,depth+1,budget,false)?; value(r,h&15,depth+1,budget,false)?; } }
            }
            12 => loop {
                let h=byte(r)?; if h==0 { break; }
                if h>>4==0 { varint(r)?; }
                value(r,h&15,depth+1,budget,true)?;
            },
            _ => return Err("invalid compact type".into()),
        }
        Ok(())
    }
    value(input,12,0,&mut 1_000_000,true)
}

fn open_descriptor(fd: i32, projection: &[u32]) -> Result<Box<ParquetScan>, String> {
    let mut file = duplicate_file(fd).map_err(|e| e.to_string())?;
    let len = file.metadata().map_err(|e| e.to_string())?.len();
    if len < 12 { return Err("truncated Parquet file".into()); }
    file.seek(SeekFrom::End(-8)).map_err(|e| e.to_string())?;
    let mut footer = [0u8;8]; file.read_exact(&mut footer).map_err(|e| e.to_string())?;
    let footer_len = u32::from_le_bytes(footer[..4].try_into().unwrap()) as u64;
    if &footer[4..] != b"PAR1" || footer_len > 16 * 1024 * 1024 || footer_len + 12 > len {
        return Err("invalid or oversized Parquet footer".into());
    }
    file.seek(SeekFrom::Start(len-footer_len-8)).map_err(|e| e.to_string())?;
    compact_preflight(&mut (&mut file).take(footer_len))?;
    let mut inspect = file.try_clone().map_err(|e| e.to_string())?;
    let reader = SerializedFileReader::new(file).map_err(|e| e.to_string())?;
    // Validate page declarations before the upstream decoder reserves compressed
    // or decompressed buffers. Scan headers only; seek past encoded payloads.
    for group in reader.metadata().row_groups() {
        for (index, column) in group.columns().iter().enumerate() {
            if !projection.is_empty() && !projection.contains(&(index as u32)) { continue; }
            let (start, length) = column.byte_range();
            let end = start.checked_add(length).ok_or("Parquet chunk offset overflow")?;
            if end > len - footer_len - 8 { return Err("Parquet chunk overlaps footer".into()); }
            inspect.seek(SeekFrom::Start(start)).map_err(|e| e.to_string())?;
            while inspect.stream_position().map_err(|e| e.to_string())? < end {
                let header_start = inspect.stream_position().map_err(|e| e.to_string())?;
                compact_preflight(&mut (&mut inspect).take((end-header_start).min(1024*1024)))?;
                inspect.seek(SeekFrom::Start(header_start)).map_err(|e| e.to_string())?;
                let header = {
                    let mut bounded = (&mut inspect).take((end-header_start).min(1024*1024));
                    let mut protocol = TCompactInputProtocol::new(&mut bounded);
                    PageHeader::read_from_in_protocol(&mut protocol).map_err(|e| e.to_string())?
                };
                let compressed = header.compressed_page_size;
                let expanded = header.uncompressed_page_size;
                if compressed < 0 || expanded < 0 || compressed > 64*1024*1024 || expanded > 64*1024*1024 {
                    return Err("Parquet page exceeds 64 MiB limit or has invalid size".into());
                }
                let position = inspect.stream_position().map_err(|e| e.to_string())?;
                let next = position.checked_add(compressed as u64).ok_or("Parquet page offset overflow")?;
                if next > end || next <= header_start { return Err("invalid Parquet page boundary".into()); }
                inspect.seek(SeekFrom::Start(next)).map_err(|e| e.to_string())?;
            }
        }
    }
    let row_count = u64::try_from(reader.metadata().file_metadata().num_rows()).map_err(|_| "negative Parquet row count")?;
    let root = reader.metadata().file_metadata().schema();
    let fields = root.get_fields();
    if fields.len() > 4096 { return Err("Parquet exceeds 4096 columns".into()); }
    let mut schema = Vec::with_capacity(fields.len());
    for field in fields {
        if !field.is_primitive() || field.get_basic_info().repetition() == Repetition::REPEATED {
            return Err("nested or repeated Parquet columns are unsupported".into());
        }
        let kind = match field.get_basic_info().converted_type() {
            ConvertedType::DATE => 4,
            ConvertedType::TIMESTAMP_MILLIS | ConvertedType::TIMESTAMP_MICROS => 5,
            _ => match field.get_physical_type() {
                Physical::BOOLEAN => 1,
                Physical::INT32 | Physical::INT64 => 2,
                Physical::FLOAT | Physical::DOUBLE => 3,
                Physical::BYTE_ARRAY | Physical::FIXED_LEN_BYTE_ARRAY => 6,
                _ => return Err("unsupported Parquet physical type".into()),
            },
        };
        schema.push(ffi::Column { name: field.name().into(), kind,
            nullable: field.get_basic_info().repetition() == Repetition::OPTIONAL });
    }
    let mut seen_projection = std::collections::HashSet::new();
    for &index in projection {
        if !seen_projection.insert(index) { return Err("duplicate Parquet projected column".into()); }
    }
    let selected = if projection.is_empty() { None } else {
        let mut list = Vec::with_capacity(projection.len());
        for &index in projection {
            list.push(fields.get(index as usize).ok_or("invalid Parquet projection")?.clone());
        }
        Some(Type::group_type_builder(root.name()).with_fields(list).build().map_err(|e| e.to_string())?)
    };
    let iterator = RowIter::from_file_into(Box::new(reader)).project(selected)
        .map_err(|e| e.to_string())?.with_batch_size(256);
    Ok(Box::new(ParquetScan { iterator: Some(iterator), error: String::new(), schema, row_count }))
}
fn open_checked(fd: i32, projection: &[u32]) -> Box<ParquetScan> {
    match open_descriptor(fd,projection) {
        Ok(scan) => scan,
        Err(error) => Box::new(ParquetScan { iterator: None, error, schema: Vec::new(), row_count: 0 }),
    }
}
impl ParquetScan {
    fn error(&self) -> &str { &self.error }
    fn read_checked(&mut self) -> ffi::Row {
        match self.next_row() {
            Ok(row) => row,
            Err(error) => ffi::Row { eof: true, cells: Vec::new(), error },
        }
    }
    fn row_count(&self) -> u64 { self.row_count }
    fn columns(&self) -> Vec<ffi::Column> {
        self.schema.iter().map(|c| ffi::Column { name: c.name.clone(), kind: c.kind, nullable: c.nullable }).collect()
    }
    fn next_row(&mut self) -> Result<ffi::Row,String> {
        let row = match self.iterator.as_mut().ok_or_else(|| self.error.clone())?.next() {
            None => return Ok(ffi::Row { eof: true, cells: Vec::new(), error: String::new() }),
            Some(value) => value.map_err(|e| e.to_string())?,
        };
        let mut cells = Vec::with_capacity(row.len());
        let mut bytes = 0usize;
        for (_, value) in row.get_column_iter() {
            let mut cell = ffi::Cell { null: false, kind: 0, integer: 0, floating: 0.0, bytes: Vec::new() };
            match value {
                Field::Null => cell.null = true,
                Field::Bool(v) => { cell.kind = 1; cell.integer = i64::from(*v); }
                Field::Byte(v) => { cell.kind = 2; cell.integer = i64::from(*v); }
                Field::Short(v) => { cell.kind = 2; cell.integer = i64::from(*v); }
                Field::Int(v) => { cell.kind = 2; cell.integer = i64::from(*v); }
                Field::Long(v) => { cell.kind = 2; cell.integer = *v; }
                Field::UByte(v) => { cell.kind = 2; cell.integer = i64::from(*v); }
                Field::UShort(v) => { cell.kind = 2; cell.integer = i64::from(*v); }
                Field::UInt(v) => { cell.kind = 2; cell.integer = i64::from(*v); }
                Field::ULong(v) => { cell.kind = 2; cell.integer = (*v).try_into().map_err(|_| "unsigned Parquet integer exceeds BIGINT")?; }
                Field::Float(v) => { cell.kind = 3; cell.floating = f64::from(*v); }
                Field::Double(v) => { cell.kind = 3; cell.floating = *v; }
                Field::Date(v) => { cell.kind = 4; cell.integer = i64::from(*v); }
                Field::TimestampMillis(v) => { cell.kind = 5; cell.integer = v.checked_mul(1000).ok_or("timestamp overflow")?; }
                Field::TimestampMicros(v) => { cell.kind = 5; cell.integer = *v; }
                Field::Str(v) => { cell.kind = 6; cell.bytes = v.as_bytes().to_vec(); }
                Field::Bytes(v) => { cell.kind = 6; cell.bytes = v.data().to_vec(); }
                _ => return Err("unsupported Parquet logical value".into()),
            }
            if cell.kind == 3 && !cell.floating.is_finite() { return Err("nonfinite Parquet number".into()); }
            bytes += cell.bytes.len();
            if bytes > 16 * 1024 * 1024 { return Err("Parquet row exceeds 16 MiB".into()); }
            cells.push(cell);
        }
        Ok(ffi::Row { eof: false, cells, error: String::new() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;
    #[test]
    fn bounded_compact_declarations() {
        // field1 binary, length 2^32-1; no payload should ever be allocated.
        assert!(compact_preflight(&mut &[0x18,0xff,0xff,0xff,0xff,0x0f][..]).is_err());
        assert!(compact_preflight(&mut &[0x19,0xf5,0xa1,0x8d,0x06][..]).is_err());
        assert!(compact_preflight(&mut &[0x15,0x80][..]).is_err());
        let mut deep=vec![0x1c;40]; deep.extend(vec![0;41]);
        assert!(compact_preflight(&mut deep.as_slice()).is_err());
        assert!(compact_preflight(&mut &[0x15,0x02,0][..]).is_ok());
    }
    #[test]
    fn rejects_malicious_footer_before_upstream_allocation() {
        use std::io::Write;
        let path=std::env::temp_dir().join(format!("parquet-bounded-footer-{}",std::process::id()));
        let footer=[0x18u8,0xff,0xff,0xff,0xff,0x0f];
        let mut f=File::create(&path).unwrap();
        f.write_all(b"PAR1").unwrap();f.write_all(&footer).unwrap();
        f.write_all(&(footer.len() as u32).to_le_bytes()).unwrap();f.write_all(b"PAR1").unwrap();drop(f);
        let f=File::open(&path).unwrap();
        let error=match open_descriptor(f.as_raw_fd(),&[]) { Ok(_)=>panic!("accepted oversized declaration"),Err(e)=>e };
        assert!(error.contains("compact byte length"),"{error}");
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn official_fixture_rows_and_projection() {
        let file = File::open(concat!(env!("CARGO_MANIFEST_DIR"),"/tests/data/simple.parquet")).unwrap();
        for projection in [Vec::new(), vec![0]] {
            let mut scan = open_descriptor(file.as_raw_fd(), &projection).unwrap();
            assert_eq!(scan.row_count(),7);
            let schema = scan.columns();
            assert_eq!(schema.len(),1);
            assert_eq!(schema[0].name,"c1");
            let mut values = Vec::new();
            loop {
                let row = scan.next_row().unwrap();
                if row.eof { break; }
                assert_eq!(row.cells.len(),1);
                assert!(!row.cells[0].null);
                values.push(row.cells[0].integer);
            }
            assert_eq!(values,vec![0,1,2,3,4,5,6]);
        }
    }
    #[test]
    fn rejects_invalid_footer_and_projection() {
        use std::io::Write;
        let temp = std::env::temp_dir().join(format!("seekdb-parquet-errors-{}",std::process::id()));
        std::fs::create_dir_all(&temp).unwrap();
        for (name, bytes) in [("short", b"PAR1".to_vec()),
            ("oversized", [b"PAR1".as_slice(), &u32::MAX.to_le_bytes(), b"PAR1".as_slice()].concat())] {
            let path = temp.join(name);
            File::create(&path).unwrap().write_all(&bytes).unwrap();
            let file = File::open(&path).unwrap();
            assert!(open_descriptor(file.as_raw_fd(),&[]).is_err());
            std::fs::remove_file(path).unwrap();
        }
        let file = File::open(concat!(env!("CARGO_MANIFEST_DIR"),"/tests/data/simple.parquet")).unwrap();
        assert!(open_descriptor(file.as_raw_fd(),&[99]).is_err());
        std::fs::remove_dir(temp).unwrap();
    }

    #[test]
    fn multiple_groups_nullable_strings_and_compression() {
        use parquet::file::writer::SerializedFileWriter;
        use parquet::file::properties::WriterProperties;
        use parquet::schema::parser::parse_message_type;
        use parquet::data_type::{Int64Type,ByteArrayType,ByteArray};
        use parquet::basic::Compression;
        use std::sync::Arc;
        for (name, compression, dictionary) in [("plain",Compression::UNCOMPRESSED,false),("snappy",Compression::SNAPPY,true)] {
            let path = std::env::temp_dir().join(format!("seekdb-multigroup-{}-{name}.parquet",std::process::id()));
            let schema = Arc::new(parse_message_type("message schema { REQUIRED INT64 id; OPTIONAL BINARY label (UTF8); }").unwrap());
            let props = Arc::new(WriterProperties::builder().set_compression(compression).set_dictionary_enabled(dictionary).build());
            let mut writer = SerializedFileWriter::new(File::create(&path).unwrap(),schema,props).unwrap();
            for group in 0..3i64 {
                let mut rg = writer.next_row_group().unwrap();
                let mut column = rg.next_column().unwrap().unwrap();
                column.typed::<Int64Type>().write_batch(&[group*3,group*3+1,group*3+2],None,None).unwrap();
                column.close().unwrap();
                let mut column = rg.next_column().unwrap().unwrap();
                let values = [ByteArray::from(format!("g{group}a").as_str()),ByteArray::from(format!("g{group}c").as_str())];
                column.typed::<ByteArrayType>().write_batch(&values,Some(&[1,0,1]),None).unwrap();
                column.close().unwrap(); rg.close().unwrap();
            }
            writer.close().unwrap();
            let file = File::open(&path).unwrap();
            for projection in [Vec::new(),vec![1]] {
                let projected = !projection.is_empty();
                let mut scan = open_descriptor(file.as_raw_fd(),&projection).unwrap();
                assert!(scan.columns()[1].nullable);
                for index in 0..9i64 {
                    let row = scan.next_row().unwrap(); assert!(!row.eof);
                    assert_eq!(row.cells.len(),if projected {1} else {2});
                    if !projected { assert_eq!(row.cells[0].integer,index); }
                    let label = &row.cells[if projected {0} else {1}];
                    assert_eq!(label.null,index%3 == 1);
                    if !label.null {
                        assert_eq!(label.bytes,format!("g{}{}",index/3,if index%3 == 0 {'a'} else {'c'}).as_bytes());
                    }
                }
                assert!(scan.next_row().unwrap().eof);
            }
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn boolean_double_date_and_timestamp() {
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;
        use parquet::data_type::{BoolType,DoubleType,Int32Type,Int64Type};
        use std::sync::Arc;
        let path = std::env::temp_dir().join(format!("seekdb-types-{}.parquet",std::process::id()));
        let schema = Arc::new(parse_message_type("message schema { REQUIRED BOOLEAN flag; REQUIRED DOUBLE number; REQUIRED INT32 day (DATE); REQUIRED INT64 stamp (TIMESTAMP_MICROS); }").unwrap());
        let mut writer = SerializedFileWriter::new(File::create(&path).unwrap(),schema,Default::default()).unwrap();
        let mut group = writer.next_row_group().unwrap();
        let mut c = group.next_column().unwrap().unwrap(); c.typed::<BoolType>().write_batch(&[true,false],None,None).unwrap(); c.close().unwrap();
        let mut c = group.next_column().unwrap().unwrap(); c.typed::<DoubleType>().write_batch(&[1.25,-2.5],None,None).unwrap(); c.close().unwrap();
        let mut c = group.next_column().unwrap().unwrap(); c.typed::<Int32Type>().write_batch(&[0,20000],None,None).unwrap(); c.close().unwrap();
        let mut c = group.next_column().unwrap().unwrap(); c.typed::<Int64Type>().write_batch(&[1000000,1700000000123456],None,None).unwrap(); c.close().unwrap();
        group.close().unwrap();writer.close().unwrap();
        let file = File::open(&path).unwrap();let mut scan = open_descriptor(file.as_raw_fd(),&[]).unwrap();
        assert_eq!(scan.columns().iter().map(|c|c.kind).collect::<Vec<_>>(),vec![1,3,4,5]);
        for i in 0..2 {
            let row = scan.next_row().unwrap();assert!(!row.eof);
            assert_eq!(row.cells[0].integer,[1,0][i]);
            assert_eq!(row.cells[1].floating,[1.25,-2.5][i]);
            assert_eq!(row.cells[2].integer,[0,20000][i]);
            assert_eq!(row.cells[3].integer,[1000000,1700000000123456][i]);
        }
        assert!(scan.next_row().unwrap().eof);std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn checked_bridge_reports_error_without_throwing() {
        let mut scan = open_checked(-1,&[]);
        assert!(!scan.error().is_empty());
        assert!(!scan.read_checked().error.is_empty());
        let file = File::open(concat!(env!("CARGO_MANIFEST_DIR"),"/tests/data/simple.parquet")).unwrap();
        let mut scan = open_checked(file.as_raw_fd(),&[]);
        assert!(scan.error().is_empty());
        let row = scan.read_checked();
        assert!(row.error.is_empty()); assert!(!row.eof); assert_eq!(row.cells[0].integer,0);
    }

}
