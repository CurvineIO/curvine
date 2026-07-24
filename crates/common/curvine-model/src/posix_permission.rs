use num_enum::{FromPrimitive, IntoPrimitive};

#[repr(i8)]
#[derive(Debug, IntoPrimitive, FromPrimitive, PartialEq, Eq, Hash, Copy, Clone)]
pub enum PosixAction {
    #[default]
    None = 1,
    Execute = 2,
    Write = 3,
    WriteExecute = 4,
    Read = 5,
    ReadExecute = 6,
    ReadWrite = 7,
    All = 8,
}

impl From<&str> for PosixAction {
    fn from(s: &str) -> Self {
        match s {
            "---" => PosixAction::None,
            "--x" => PosixAction::Execute,
            "-w-" => PosixAction::Write,
            "-wx" => PosixAction::WriteExecute,
            "r--" => PosixAction::Read,
            "r-x" => PosixAction::ReadExecute,
            "rw-" => PosixAction::ReadWrite,
            "rwx" => PosixAction::All,
            _ => PosixAction::None,
        }
    }
}

pub struct PosixPermission {
    pub user_act: PosixAction,
    pub group_act: PosixAction,
    pub other_act: PosixAction,
}

impl PosixPermission {
    pub fn new(user_act: PosixAction, group_act: PosixAction, other_act: PosixAction) -> Self {
        Self {
            user_act,
            group_act,
            other_act,
        }
    }
}
