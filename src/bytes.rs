use std::io;
use std::io::{Read, Write};
use std::marker::PhantomData;

pub trait WriteBytes {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()>;

    fn write_bytes_with_writer(&self, writer: impl Write) -> io::Result<usize> {
        let mut writer = BytesWriter::new(writer);
        self.write_bytes(&mut writer)?;
        Ok(writer.bytes_written())
    }
}

pub trait TryFromBytes: Sized {
    fn try_from_bytes(bytes: impl ReadBytes) -> Option<Self>;

    fn try_from_slice(bytes: &[u8]) -> Option<Self> {
        Self::try_from_bytes(BytesRef::new(bytes))
    }

    fn try_from_reader(reader: impl Read) -> Option<Self> {
        Self::try_from_bytes(ByteReader::new(reader))
    }
}

#[derive(Copy, Clone)]
pub struct BytesRef<'a> {
    bytes: &'a [u8],
    read: usize,
}

pub struct ByteReader<T> {
    reader: T,
    buf: Vec<u8>,
    read: usize,
}

impl<'a> BytesRef<'a> {
    pub fn new(bytes: &'a [u8]) -> BytesRef<'a> {
        BytesRef {
            bytes,
            read: 0,
        }
    }

    pub fn remaining_ref(&mut self) -> &'a [u8] {
        self.next_bytes_ref(self.bytes.len()).unwrap_or(&[])
    }

    pub fn next_bytes_ref(&mut self, amt: usize) -> Option<&'a [u8]> {
        if self.bytes.len() < amt {
            None
        } else {
            let ret = &self.bytes[..amt];
            self.bytes = &self.bytes[amt..];
            self.read += amt;
            Some(ret)
        }
    }
}

impl<T: io::Read> ByteReader<T> {
    pub fn new(reader: T) -> ByteReader<T> {
        ByteReader {
            reader,
            buf: vec![],
            read: 0,
        }
    }
}

impl<'a> ReadBytes for BytesRef<'a> {
    fn remaining(&mut self) -> &[u8] {
        self.remaining_ref()
    }

    fn next_bytes(&mut self, amt: usize) -> Option<&[u8]> {
        self.next_bytes_ref(amt)
    }

    fn read_amount(&self) -> usize {
        self.read
    }
}

impl<T: io::Read> ReadBytes for ByteReader<T> {
    fn remaining(&mut self) -> &[u8] {
        self.buf.clear();
        if let Ok(amt) = self.reader.read_to_end(&mut self.buf) {
            self.read += amt;
            &self.buf
        } else {
            &[]
        }
    }

    fn next_bytes(&mut self, amt: usize) -> Option<&[u8]> {
        self.buf.clear();
        self.buf.resize_with(amt, || 0u8);
        if let Ok(()) = self.reader.read_exact(&mut self.buf) {
            self.read += amt;
            Some(&self.buf[..amt])
        } else {
            None
        }
    }

    fn read_amount(&self) -> usize {
        self.read
    }
}

pub trait ReadBytes: Sized {
    fn remaining(&mut self) -> &[u8];

    fn next_bytes(&mut self, amt: usize) -> Option<&[u8]>;

    fn read_amount(&self) -> usize;

    fn next<T: TryFromBytes>(&mut self) -> Option<T> {
        T::try_from_bytes(self)
    }

    fn iter<T: TryFromBytes>(&mut self) -> BytesIterator<&mut Self, T> {
        BytesIterator(self, PhantomData)
    }

    fn next_u8(&mut self) -> Option<u8> {
        let bytes = self.next_bytes(1)?;
        Some(bytes[0])
    }

    fn next_u16(&mut self) -> Option<u16> {
        let bytes = self.next_bytes(2)?;
        Some(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn next_u32(&mut self) -> Option<u32> {
        let bytes = self.next_bytes(4)?;
        Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn next_u64(&mut self) -> Option<u64> {
        let bytes = self.next_bytes(8)?;
        Some(u64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]))
    }

    fn next_u128(&mut self) -> Option<u128> {
        let bytes = self.next_bytes(16)?;
        Some(u128::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]]))
    }

    fn next_bool(&mut self) -> Option<bool> {
        let byte = self.next_u8()?;
        Some(byte != 0)
    }
}

impl<T: ReadBytes> ReadBytes for &mut T {
    fn remaining(&mut self) -> &[u8] {
        (**self).remaining()
    }

    fn next_bytes(&mut self, amt: usize) -> Option<&[u8]> {
        (**self).next_bytes(amt)
    }

    fn read_amount(&self) -> usize {
        (**self).read_amount()
    }
}

pub struct BytesIterator<R, T>(R, PhantomData<T>);

impl<R: ReadBytes, T: TryFromBytes> Iterator for BytesIterator<R, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct BytesWriter<W> {
    writer: W,
    written: usize,
}

impl<W> BytesWriter<W> {
    pub fn new(writer: W) -> BytesWriter<W> {
        BytesWriter {
            writer,
            written: 0,
        }
    }
}

impl<W: Write> BytesWriter<W> {
    pub fn write_u8(&mut self, n: u8) -> io::Result<()> {
        self.write(&[n])
    }

    pub fn write_u16(&mut self, n: u16) -> io::Result<()> {
        self.write(&n.to_be_bytes())
    }

    pub fn write_u32(&mut self, n: u32) -> io::Result<()> {
        self.write(&n.to_be_bytes())
    }

    pub fn write_u64(&mut self, n: u64) -> io::Result<()> {
        self.write(&n.to_be_bytes())
    }

    pub fn write_bool(&mut self, b: bool) -> io::Result<()> {
        self.write(if b { &[1] } else { &[0] })
    }

    pub fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.written += self.writer.write(data)?;
        Ok(())
    }

    pub fn bytes_written(&self) -> usize {
        self.written
    }
}