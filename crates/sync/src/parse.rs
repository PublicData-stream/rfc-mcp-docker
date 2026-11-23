//! Parse work and its owned handoff to the single database writer.

use crate::{
    Chunk, Section,
    pipeline::Shutdown,
    text::{self, TextIndex},
};
use rfc_core::ByteRange;
use rfc_storage::postgres::Result;
use std::fmt;

pub(crate) const DOCUMENT_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct BudgetExceeded;

impl fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("document parse budget exceeded")
    }
}

impl std::error::Error for BudgetExceeded {}

pub(crate) struct Budget<'a> {
    pub bytes: usize,
    pub shutdown: &'a Shutdown,
}

impl Budget<'_> {
    pub(crate) fn reserve(&self, bytes: usize) -> Result<()> {
        self.shutdown.check()?;
        if bytes > self.bytes {
            return Err(BudgetExceeded.into());
        }
        Ok(())
    }
}

pub(crate) struct OwnedText {
    pub headings: Vec<String>,
    pub data: TextData,
}

pub(crate) struct TextData {
    index: TextIndex<'static>,
    section_numbers: Vec<Option<ByteRange>>,
    pub retained_bytes: usize,
}

impl OwnedText {
    pub(crate) fn parse(bytes: &[u8], budget: &Budget<'_>) -> Result<Self> {
        let indexed = text::index_with_budget(bytes, budget)?;
        validate_ranges(bytes, &indexed)?;
        let retained_bytes = indexed.line_starts.capacity() * size_of::<i64>()
            + indexed.sections.len()
                * (size_of::<Section<'_>>() + size_of::<Option<ByteRange>>() + 128)
            + indexed.headings.iter().map(String::capacity).sum::<usize>()
            + indexed.headings.capacity() * size_of::<String>()
            + indexed.chunks.len() * size_of::<Chunk<'_>>()
            + indexed.notices.capacity() * size_of::<crate::Notice<'_>>()
            + indexed.references.capacity() * size_of::<text::TextReference>();
        budget.reserve(retained_bytes)?;
        let mut section_numbers = Vec::with_capacity(indexed.sections.len());
        let mut sections = Vec::with_capacity(indexed.sections.len());
        for section in indexed.sections {
            section_numbers.push(section.number.map(|number| {
                let start = number.as_ptr().addr() - bytes.as_ptr().addr();
                ByteRange::new(start as i64, (start + number.len()) as i64)
                    .expect("TXT number range")
            }));
            sections.push(Section {
                id: section.id,
                parent: section.parent,
                ordinal: section.ordinal,
                depth: section.depth,
                number: None,
                heading: "",
                body: section.body,
                raw_heading: section.raw_heading,
                parse_state: section.parse_state,
            });
        }
        let index = TextIndex {
            text: indexed.text.map(|_| ""),
            invalid_utf8: indexed.invalid_utf8,
            contains_nul: indexed.contains_nul,
            line_starts: indexed.line_starts,
            sections,
            headings: Vec::new(),
            section_state: indexed.section_state,
            notices: indexed.notices,
            notice_state: indexed.notice_state,
            notice_family: indexed.notice_family,
            chunks: indexed
                .chunks
                .into_iter()
                .map(|chunk| Chunk {
                    range: chunk.range,
                    text: "",
                })
                .collect(),
            references: indexed.references,
            all_references: indexed.all_references,
            classified_references: indexed.classified_references,
        };
        Ok(Self {
            headings: indexed.headings,
            data: TextData {
                index,
                section_numbers,
                retained_bytes,
            },
        })
    }
}

impl TextData {
    pub(crate) fn into_view<'a>(
        self,
        bytes: &'a [u8],
        headings: &'a [String],
    ) -> Result<TextIndex<'a>> {
        let mut index: TextIndex<'a> = self.index;
        let slice = |range: ByteRange| -> Result<&'a str> {
            Ok(std::str::from_utf8(
                range
                    .select(bytes)
                    .ok_or("TXT range exceeds source bytes")?,
            )?)
        };
        if index.text.is_some() {
            index.text = Some(std::str::from_utf8(bytes)?);
        }
        for ((section, number), heading) in index
            .sections
            .iter_mut()
            .zip(self.section_numbers)
            .zip(headings)
        {
            section.heading = heading;
            section.number = number.map(slice).transpose()?;
        }
        for chunk in &mut index.chunks {
            chunk.text = slice(chunk.range)?;
        }
        Ok(index)
    }
}

fn validate_ranges(bytes: &[u8], indexed: &TextIndex<'_>) -> Result<()> {
    let valid = |range: ByteRange| {
        indexed
            .text
            .and_then(|text| text.get(range.start() as usize..range.end() as usize))
            .is_some_and(|slice| !slice.is_empty())
    };
    if indexed.sections.iter().any(|section| {
        !valid(section.raw_heading)
            || !valid(section.body)
            || section.raw_heading.start() != section.body.start()
            || section.raw_heading.end() > section.body.end()
    }) || indexed.notices.iter().any(|notice| !valid(notice.range))
        || indexed
            .references
            .iter()
            .any(|evidence| !valid(evidence.range))
    {
        return Err("TXT parser produced an invalid source range".into());
    }
    let mut cursor = 0;
    for chunk in &indexed.chunks {
        if chunk.range.start() != cursor || chunk.range.select(bytes) != Some(chunk.text.as_bytes())
        {
            return Err("TXT chunk does not match its exact source bytes".into());
        }
        cursor = chunk.range.end();
    }
    if indexed.text.is_some() && cursor != bytes.len() as u64 {
        return Err("TXT chunks do not cover the verified blob".into());
    }
    Ok(())
}
