use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, RpcCode};
use curvine_model::{FileType, SetAttrOpts};
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum ChmodCommand {
    Chmod {
        #[clap(
            allow_hyphen_values = true,
            help = "Permissions to set: octal (755, 0755, 0o755) or symbolic \
                    [ugoa]*[+-=][rwx]*, comma-separated (u+x, go-w, a=r,u+w)"
        )]
        mode: String,
        #[clap(help = "Path of the file/directory to modify")]
        path: String,
        #[clap(help = "Recursively apply permissions to all files and directories")]
        recursive: bool,
    },
}

impl ChmodCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            ChmodCommand::Chmod {
                mode,
                path,
                recursive,
            } => {
                let path = CurvineURI::new(path)?;

                // Parse before any RPC so that an invalid mode never changes a path.
                match Mode::parse(mode)? {
                    Mode::Octal(mode) => {
                        let opts = SetAttrOpts {
                            mode: Some(mode),
                            recursive: *recursive,
                            ..Default::default()
                        };
                        client.set_attr(&path, opts).await?;
                        println!("Changed permission of '{}' set to {:o}", path, mode);
                    }

                    Mode::Symbolic(clauses) => {
                        let status = client.get_status(&path).await?;
                        let new_mode = apply_clauses(&clauses, status.mode);
                        set_mode(&client, &path, new_mode).await?;

                        // A symbolic mode is relative to each path's own mode, so the
                        // master-side recursive set_attr (one mode for the whole
                        // subtree) cannot be used here.
                        if *recursive && status.is_dir {
                            chmod_children(&client, &path, &clauses).await?;
                        }
                        println!("Changed permission of '{}' set to {:o}", path, new_mode);
                    }
                }

                Ok(())
            }
        }
    }
}

async fn set_mode(client: &UnifiedFileSystem, path: &CurvineURI, mode: u32) -> CommonResult<()> {
    let opts = SetAttrOpts {
        mode: Some(mode),
        ..Default::default()
    };
    client.set_attr(path, opts).await?;
    Ok(())
}

/// Applies `clauses` to every entry below `dir`, each relative to its own mode.
/// Symbolic links are skipped, like `chmod -R` does.
async fn chmod_children(
    client: &UnifiedFileSystem,
    dir: &CurvineURI,
    clauses: &[Clause],
) -> CommonResult<()> {
    let mut dirs = vec![dir.clone()];
    while let Some(dir) = dirs.pop() {
        // Paths under a cache-mode mount are listed from the UFS and set_attr
        // ignores them, so walking such a subtree would change nothing.
        if client
            .get_mount_checked(&dir, RpcCode::ListStatus)
            .await?
            .is_some()
        {
            continue;
        }

        for child in client.list_status(&dir).await? {
            if child.file_type == FileType::Link {
                continue;
            }

            let child_path = CurvineURI::new(&child.path)?;
            set_mode(client, &child_path, apply_clauses(clauses, child.mode)).await?;
            if child.is_dir {
                dirs.push(child_path);
            }
        }
    }
    Ok(())
}

/// Permission bits a symbolic mode can address. Setuid, setgid and sticky bits
/// are outside this mask and are always preserved.
const PERM_MASK: u32 = 0o777;

/// A mode argument of `cv fs chmod`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    /// An absolute octal mode such as `755`, `0755` or `0o755`.
    Octal(u32),
    /// Comma-separated symbolic clauses such as `u+x,go-w`, applied in order to
    /// the current mode of each path.
    Symbolic(Vec<Clause>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Add,
    Remove,
    Set,
}

/// One `[ugoa]*[+-=][rwx]*` action. `who` and `perms` are masks over
/// [`PERM_MASK`]; the bits a clause touches are `who & perms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Clause {
    who: u32,
    op: Op,
    perms: u32,
}

impl Clause {
    fn apply(&self, mode: u32) -> u32 {
        let bits = self.who & self.perms;
        match self.op {
            Op::Add => mode | bits,
            Op::Remove => mode & !bits,
            Op::Set => (mode & !self.who) | bits,
        }
    }
}

fn apply_clauses(clauses: &[Clause], mode: u32) -> u32 {
    clauses
        .iter()
        .fold(mode & 0o7777, |mode, clause| clause.apply(mode))
}

impl Mode {
    fn parse(mode_str: &str) -> CommonResult<Self> {
        let mode_str = mode_str.trim();
        if mode_str.is_empty() {
            return Err("Mode string is empty".into());
        }

        let s = mode_str.strip_prefix("0o").unwrap_or(mode_str);
        let s = if s.len() == 4 && s.starts_with('0') {
            &s[1..]
        } else {
            s
        };

        if let Ok(m) = u32::from_str_radix(s, 8) {
            return Ok(Mode::Octal(m));
        }

        let mut clauses = vec![];
        for part in mode_str.split(',') {
            Self::parse_part(part, &mut clauses)?;
        }
        Ok(Mode::Symbolic(clauses))
    }

    /// Parses `[ugoa]*([+-=][rwx]*)+`. An empty `who` means `a`.
    fn parse_part(part: &str, clauses: &mut Vec<Clause>) -> CommonResult<()> {
        let op_start = part
            .find(['+', '-', '='])
            .ok_or_else(|| format!("Invalid mode part '{}': expected +, - or =", part))?;

        let mut who = 0;
        for c in part[..op_start].chars() {
            who |= match c {
                'u' => 0o700,
                'g' => 0o070,
                'o' => 0o007,
                'a' => PERM_MASK,
                _ => {
                    return Err(
                        format!("Invalid 'who' character '{}' in mode part '{}'", c, part).into(),
                    )
                }
            };
        }
        if who == 0 {
            who = PERM_MASK;
        }

        // `part[op_start..]` starts with an operator, and the inner loop stops
        // before the next one, so the outer loop only ever sees operators.
        let mut actions = part[op_start..].chars().peekable();
        while let Some(op) = actions.next() {
            let op = match op {
                '+' => Op::Add,
                '-' => Op::Remove,
                _ => Op::Set,
            };

            let mut perms = 0;
            while let Some(c) = actions.next_if(|c| !matches!(c, '+' | '-' | '=')) {
                perms |= match c {
                    'r' => 0o444,
                    'w' => 0o222,
                    'x' => 0o111,
                    _ => {
                        return Err(format!(
                            "Invalid permission character '{}' in mode part '{}'",
                            c, part
                        )
                        .into())
                    }
                };
            }
            clauses.push(Clause { who, op, perms });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(mode: &str) -> Mode {
        Mode::parse(mode).unwrap_or_else(|e| panic!("{mode}: {e}"))
    }

    /// Resolves `mode` against `current` the way `cv fs chmod` does.
    fn resolve(mode: &str, current: u32) -> u32 {
        match parse(mode) {
            Mode::Octal(m) => m,
            Mode::Symbolic(clauses) => apply_clauses(&clauses, current),
        }
    }

    fn parse_err(mode: &str) -> String {
        match Mode::parse(mode) {
            Ok(m) => panic!("{mode} should be rejected, parsed as {m:?}"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn octal_modes_are_unchanged() {
        assert_eq!(parse("755"), Mode::Octal(0o755));
        assert_eq!(parse("0755"), Mode::Octal(0o755));
        assert_eq!(parse("0o755"), Mode::Octal(0o755));
        assert_eq!(parse(" 644 "), Mode::Octal(0o644));
        assert_eq!(parse("1777"), Mode::Octal(0o1777));
        assert_eq!(parse("0"), Mode::Octal(0));
    }

    #[test]
    fn add_sets_only_the_selected_bits() {
        assert_eq!(resolve("u+x", 0o644), 0o744);
        assert_eq!(resolve("ug+rw", 0o400), 0o660);
        assert_eq!(resolve("+x", 0o644), 0o755);
        assert_eq!(resolve("a+x", 0o644), 0o755);
    }

    #[test]
    fn remove_clears_only_the_selected_bits() {
        assert_eq!(resolve("go-w", 0o666), 0o644);
        assert_eq!(resolve("o-rwx", 0o777), 0o770);
        assert_eq!(resolve("u-x", 0o644), 0o644);
    }

    #[test]
    fn set_replaces_the_selected_classes() {
        assert_eq!(resolve("a=r", 0o777), 0o444);
        assert_eq!(resolve("u=rwx,g=rx", 0o644), 0o754);
        assert_eq!(resolve("o=", 0o777), 0o770);
        assert_eq!(resolve("go=", 0o755), 0o700);
    }

    #[test]
    fn unselected_bits_are_preserved() {
        assert_eq!(resolve("u+x", 0o654), 0o754);
        assert_eq!(resolve("g-w", 0o4775), 0o4755);
        assert_eq!(resolve("a=rx", 0o1777), 0o1555);
    }

    #[test]
    fn clauses_apply_in_order() {
        assert_eq!(resolve("a=r,u+w", 0o000), 0o644);
        assert_eq!(resolve("u+x,u-x", 0o644), 0o644);
        assert_eq!(resolve("u+r-w", 0o200), 0o400);
        assert_eq!(resolve("go=r+x", 0o700), 0o755);
    }

    #[test]
    fn file_type_bits_are_dropped() {
        assert_eq!(resolve("u+x", 0o100644), 0o744);
    }

    #[test]
    fn invalid_modes_are_rejected() {
        assert!(parse_err("").contains("empty"));
        assert!(parse_err("u+z").contains("Invalid permission character 'z'"));
        assert!(parse_err("x+r").contains("Invalid 'who' character 'x'"));
        assert!(parse_err("u").contains("expected +, - or ="));
        assert!(parse_err("u+x,").contains("expected +, - or ="));
        assert!(parse_err("u+x,,g+w").contains("expected +, - or ="));
        assert!(parse_err("rwx").contains("expected +, - or ="));
        assert!(parse_err("u+s").contains("Invalid permission character 's'"));
        assert!(parse_err("888").contains("expected +, - or ="));
    }
}
