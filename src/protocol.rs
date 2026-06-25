#[derive(Debug)]
pub enum OP {
    Get,
    Add,
    Update,
    Remove
}

#[derive(Debug)]
pub struct Action {
    pub op: OP,
    pub key: String,
    pub value: String
}

pub struct BitCursor<'a> {
    buf: &'a [u8],
    bit_pos: usize,
}

impl<'a> BitCursor<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, bit_pos: 0 }
    }

    pub fn stream_position(&self) -> usize {
        self.bit_pos
    }

    pub fn advance(&mut self, n: usize) -> Result<(), &'static str> {
        let new_pos = self.bit_pos.checked_add(n).ok_or("bit position overflow")?;
        if new_pos > self.buf.len() * 8 {
            return Err("advance past end of buffer");
        }
        self.bit_pos = new_pos;
        Ok(())
    }

    pub fn read_bits(&mut self, n: usize) -> Result<u64, &'static str> {
        if n > 64 {
            return Err("cannot read more than 64 bits into u64");
        }

        let end = self.bit_pos.checked_add(n).ok_or("bit position overflow")?;
        if end > self.buf.len() * 8 {
            return Err("read past end of buffer");
        }

        let mut value = 0u64;

        for _ in 0..n {
            let byte_index = self.bit_pos / 8;
            let bit_index = self.bit_pos % 8;

            let byte = self.buf[byte_index];

            // Read bits MSB-first within each byte.
            let bit = (byte >> (7 - bit_index)) & 1;

            value = (value << 1) | (bit as u64);
            self.bit_pos += 1;
        }

        Ok(value)
    }

    fn read_bytes(&mut self, n: usize) -> Result<Vec<u8>, &'static str> {
        let mut out = Vec::with_capacity(n);

        for _ in 0..n {
            out.push(self.read_bits(8)? as u8);
        }

        Ok(out)
    }
}

pub struct BitWriter {
    buf: Vec<u8>,
    bit_pos: usize,
}

impl BitWriter {
    pub fn new() -> Self {
        Self { buf: Vec::new(), bit_pos: 0 }
    }

    pub fn stream_position(&self) -> usize {
        self.bit_pos
    }

    pub fn advance(&mut self, n: usize) -> Result<(), &'static str> {
        let new_pos = self.bit_pos.checked_add(n).ok_or("bit position overflow")?;
        if new_pos > self.buf.len() * 8 {
            return Err("advance past end of buffer");
        }
        self.bit_pos = new_pos;
        Ok(())
    }

    pub fn write_bits(&mut self, value: u64, nbits: usize) -> Result<(), &'static str> {
        if nbits > 64 {
            return Err("too many bits");
        }

        for i in (0..nbits).rev() {
            let bit = ((value >> i) & 1) as u8;

            if bit > 1 {
                return Err("bit must be 0 or 1");
            }

            let byte_index = self.bit_pos / 8;
            let bit_index = self.bit_pos % 8;

            if byte_index == self.buf.len() {
                self.buf.push(0);
            }

            let shift = 7 - bit_index;

            if bit == 1 {
                self.buf[byte_index] |= 1 << shift;
            } else {
                self.buf[byte_index] &= !(1 << shift);
            }

            self.bit_pos += 1;
        }

        Ok(())
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), &'static str> {
        for &b in bytes {
            self.write_bits(b as u64, 8)?;
        }

        Ok(())
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

pub fn decode_msg(cur: &mut BitCursor) -> Action {
    let (op, has_value) = match cur.read_bits(2).unwrap() as u8 {
        0b00 => (OP::Get, false),
        0b01 => (OP::Add, true),
        0b10 => (OP::Update, true),
        0b11 => (OP::Remove, false),
        _ => panic!("unknown opcode"),
    };

    let key_len = cur.read_bits(32).unwrap() as usize;
    let key = String::from_utf8_lossy(&cur.read_bytes(key_len).unwrap()).to_string();

    let value = if has_value {
        let value_len = cur.read_bits(32).unwrap() as usize;
        String::from_utf8_lossy(&cur.read_bytes(value_len).unwrap()).to_string()
    } else {
        String::new()
    };

    Action { op, key, value }
}



pub fn write_msg(op: OP, key: &str, value: &str) -> Result<Vec<u8>, &'static str> {
    let mut cur = BitWriter::new();

    let (op_code, has_value) = match op {
        OP::Get => (0b00, false),
        OP::Add => (0b01, true),
        OP::Update => (0b10, true),
        OP::Remove => (0b11, false),
    };

    cur.write_bits(op_code, 2)?;

    cur.write_bits(key.len() as u64, 32)?;
    cur.write_bytes(key.as_bytes())?;

    if has_value {
        cur.write_bits(value.len() as u64, 32)?;
        cur.write_bytes(value.as_bytes())?;
    }

    Ok(cur.finish())
}
