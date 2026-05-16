#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Text = 0,
    Binary = 1,
}

impl Format {
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0 => Some(Format::Text),
            1 => Some(Format::Binary),
            _ => None,
        }
    }

    pub fn to_u16(self) -> u16 {
        self as u16
    }
}
