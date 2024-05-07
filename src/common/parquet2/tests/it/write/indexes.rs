use std::io::Cursor;

use parquet2::compression::CompressionOptions;
use parquet2::error::Result;
use parquet2::indexes::select_pages;
use parquet2::indexes::BoundaryOrder;
use parquet2::indexes::Index;
use parquet2::indexes::Interval;
use parquet2::indexes::NativeIndex;
use parquet2::indexes::PageIndex;
use parquet2::indexes::PageLocation;
use parquet2::metadata::SchemaDescriptor;
use parquet2::read::read_columns_indexes;
use parquet2::read::read_metadata;
use parquet2::read::read_pages_locations;
use parquet2::read::BasicDecompressor;
use parquet2::read::IndexedPageReader;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::write::Compressor;
use parquet2::write::DynIter;
use parquet2::write::DynStreamingIterator;
use parquet2::write::FileWriter;
use parquet2::write::Version;
use parquet2::write::WriteOptions;

use super::primitive::array_to_page_v1;
use crate::read::collect;
use crate::Array;

fn write_file() -> Result<Vec<u8>> {
    let page1 = vec![Some(0), Some(1), None, Some(3), Some(4), Some(5), Some(6)];
    let page2 = vec![Some(10), Some(11)];

    let options = WriteOptions {
        write_statistics: true,
        version: Version::V1,
    };

    let schema = SchemaDescriptor::new("schema".to_string(), vec![ParquetType::from_physical(
        "col1".to_string(),
        PhysicalType::Int32,
    )]);

    let pages = vec![
        array_to_page_v1::<i32>(&page1, &options, &schema.columns()[0].descriptor),
        array_to_page_v1::<i32>(&page2, &options, &schema.columns()[0].descriptor),
    ];

    let pages = DynStreamingIterator::new(Compressor::new(
        DynIter::new(pages.into_iter()),
        CompressionOptions::Uncompressed,
        vec![],
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);

    writer.write(DynIter::new(columns))?;
    writer.end(None)?;

    Ok(writer.into_inner().into_inner())
}

#[test]
fn read_indexed_page() -> Result<()> {
    let data = write_file()?;
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    let column = 0;
    let columns = &metadata.row_groups[0].columns();

    // selected the rows
    let intervals = &[Interval::new(2, 2)];

    let pages = read_pages_locations(&mut reader, columns)?;

    let pages = select_pages(intervals, &pages[column], metadata.row_groups[0].num_rows())?;

    let pages = IndexedPageReader::new(reader, &columns[column], pages, vec![], vec![]);

    let pages = BasicDecompressor::new(pages, vec![]);

    let arrays = collect(pages, columns[column].physical_type())?;

    // the second item and length 2
    assert_eq!(arrays, vec![Array::Int32(vec![None, Some(3)])]);

    Ok(())
}

#[test]
fn read_indexes_and_locations() -> Result<()> {
    let data = write_file()?;
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    let columns = &metadata.row_groups[0].columns();

    let expected_page_locations = vec![vec![
        PageLocation {
            offset: 4,
            compressed_page_size: 63,
            first_row_index: 0,
        },
        PageLocation {
            offset: 67,
            compressed_page_size: 47,
            first_row_index: 7,
        },
    ]];
    let expected_index = vec![Box::new(NativeIndex::<i32> {
        primitive_type: PrimitiveType::from_physical("col1".to_string(), PhysicalType::Int32),
        indexes: vec![
            PageIndex {
                min: Some(0),
                max: Some(6),
                null_count: Some(1),
            },
            PageIndex {
                min: Some(10),
                max: Some(11),
                null_count: Some(0),
            },
        ],
        boundary_order: BoundaryOrder::Unordered,
    }) as Box<dyn Index>];

    let indexes = read_columns_indexes(&mut reader, columns)?;
    assert_eq!(&indexes, &expected_index);

    let pages = read_pages_locations(&mut reader, columns)?;
    assert_eq!(pages, expected_page_locations);

    Ok(())
}
