use std::fmt::{Debug, Display};

pub enum Error{
  Illegal,
  UnSupport,
  Other(String),
}
impl Display for Error{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      match self {
        Self::Illegal => write!(f, "Illegal"),
        Self::UnSupport => write!(f, "UnSupport"),
        Self::Other(arg0) => f.debug_tuple("Other").field(arg0).finish(),
    }
    }
}
impl Debug for Error{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Illegal => write!(f, "Illegal"),
            Self::UnSupport => write!(f, "UnSupport"),
            Self::Other(arg0) => f.debug_tuple("Other").field(arg0).finish(),
        }
    }
}
impl std::error::Error for Error{

}