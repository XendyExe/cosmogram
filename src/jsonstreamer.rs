use serde::de::DeserializeOwned;
use serde_json::Deserializer;
use std::io::{self, BufReader, Read};

/// Transforms a `[a, b, c]` byte stream into `a b c` on the fly,
/// so serde_json's StreamDeserializer can consume elements one at a time.
struct JsonArrayReader<R: Read> {
    inner: R,
    depth: i32,
    in_string: bool,
    escape_next: bool,
    started: bool,
    done: bool,
}

impl<R: Read> JsonArrayReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            depth: 0,
            in_string: false,
            escape_next: false,
            started: false,
            done: false,
        }
    }
}

impl<R: Read> Read for JsonArrayReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.done {
            return Ok(0);
        }

        // Read a whole chunk directly into the caller's buffer
        let n = self.inner.read(buf)?;
        if n == 0 {
            return Ok(0);
        }

        // Transform in-place — every byte maps 1:1 so length stays the same
        for i in 0..n {
            let b = buf[i];

            if self.escape_next {
                self.escape_next = false;
                continue; // leave byte as-is
            }

            if self.in_string {
                match b {
                    b'\\' => self.escape_next = true,
                    b'"'  => self.in_string = false,
                    _     => {}
                }
                continue; // leave byte as-is
            }

            // Not in a string — apply structural transformations
            buf[i] = match b {
                b'"' => { self.in_string = true; b }

                b'[' if !self.started => {
                    self.started = true;
                    b' ' // swallow the opening bracket
                }

                b'[' | b'{' => { self.depth += 1; b }

                b']' if self.depth == 0 => {
                    self.done = true;
                    b' ' // swallow the closing bracket, signal EOF next call
                }

                b']' => { self.depth -= 1; b }
                b'}' => { self.depth -= 1; b }

                b',' if self.depth == 0 => b' ', // top-level separator → whitespace

                _ => b,
            };

            if self.done {
                // Zero out anything after the closing `]` in this chunk
                for j in (i + 1)..n {
                    buf[j] = b' ';
                }
                return Ok(n);
            }
        }

        Ok(n)
    }
}

pub fn stream_json_array<T, R>(reader: R) -> impl Iterator<Item = serde_json::Result<T>>
where
    T: DeserializeOwned,
    R: Read,
{
    let array_reader = JsonArrayReader::new(BufReader::new(reader));
    Deserializer::from_reader(array_reader).into_iter::<T>()
}