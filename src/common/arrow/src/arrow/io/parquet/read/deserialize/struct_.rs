use super::nested_utils::NestedArrayIter;
use super::nested_utils::NestedState;
use crate::arrow::array::Array;
use crate::arrow::array::StructArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::Field;
use crate::arrow::error::Error;

/// An iterator adapter over [`NestedArrayIter`] assumed to be encoded as Struct arrays
pub struct StructIterator<'a> {
    iters: Vec<NestedArrayIter<'a>>,
    fields: Vec<Field>,
}

impl<'a> StructIterator<'a> {
    /// Creates a new [`StructIterator`] with `iters` and `fields`.
    pub fn new(iters: Vec<NestedArrayIter<'a>>, fields: Vec<Field>) -> Self {
        assert_eq!(iters.len(), fields.len());
        Self { iters, fields }
    }
}

impl<'a> Iterator for StructIterator<'a> {
    type Item = Result<(NestedState, Box<dyn Array>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Vec<_>>();

        if values.iter().any(|x| x.is_none()) {
            return None;
        }

        // todo: unzip of Result not yet supported in stable Rust
        let mut nested = vec![];
        let mut new_values = vec![];
        for x in values {
            match x.unwrap() {
                Ok((nest, values)) => {
                    new_values.push(values);
                    nested.push(nest);
                }
                Err(e) => return Some(Err(e)),
            }
        }
        let mut nested = nested.pop().unwrap();
        let (_, validity) = nested.nested.pop().unwrap().inner();

        Some(Ok((
            nested,
            Box::new(StructArray::new(
                DataType::Struct(self.fields.clone()),
                new_values,
                validity.and_then(|x| x.into()),
            )),
        )))
    }
}
