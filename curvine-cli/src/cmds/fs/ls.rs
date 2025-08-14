use clap::Subcommand;
use curvine_client::file::CurvineFileSystem;
use curvine_common::fs::CurvineURI;
use curvine_common::state::FileStatus;
use orpc::CommonResult;

/// Configuration for printing file entries
#[derive(Debug)]
struct PrintConfig {
    human_readable: bool,
    hide_non_printable: bool,
    atime: bool,

    display_path: String,
}

#[derive(Subcommand, Debug)]
pub enum LsCommand {
    /// List files and directories
    Ls {
        #[clap(value_name = "path", default_value = "/")]
        path: String,

        #[clap(
            short = 'C',
            long,
            help = "Display the paths of files and directories only"
        )]
        path_only: bool,

        #[clap(short = 'd', long, help = "Directories are listed as plain files")]
        directory: bool,

        #[clap(
            short = 'H',
            long,
            help = "Formats the sizes of files in a human-readable fashion"
        )]
        human_readable: bool,

        #[clap(
            short = 'q',
            long,
            help = "Print ? instead of non-printable characters"
        )]
        hide_non_printable: bool,

        #[clap(
            short = 'R',
            long,
            help = "Recursively list the contents of directories"
        )]
        recursive: bool,

        #[clap(short = 'r', long, help = "Reverse the order of the sort")]
        reverse: bool,

        #[clap(
            short = 't',
            long,
            help = "Sort files by modification time (most recent first)"
        )]
        mtime: bool,

        #[clap(short = 'S', long, help = "Sort files by size")]
        size: bool,

        #[clap(
            short = 'u',
            long,
            help = "Use time of last access instead of modification for display and sorting"
        )]
        atime: bool,
    },
}

impl LsCommand {
    pub async fn execute(&self, client: CurvineFileSystem) -> CommonResult<()> {
        match self {
            LsCommand::Ls {
                path,
                path_only,
                directory,
                human_readable,
                hide_non_printable,
                recursive,
                reverse,
                mtime,
                size,
                atime,
            } => {
                let path = CurvineURI::new(path)?;

                // Handle path-only mode
                if *path_only {
                    if *recursive {
                        // Recursive path-only listing
                        list_paths_recursive(&client, &path).await?;
                    } else {
                        // Single level path-only listing
                        let files = client.list_status(&path).await?;
                        for file in files {
                            println!("{}", file.path);
                        }
                    }
                    return Ok(());
                }

                // Handle directory-only mode (treat directories as files)
                if *directory {
                    let status = client.get_status(&path).await?;
                    if status.is_dir {
                        let config = PrintConfig {
                            human_readable: *human_readable,
                            hide_non_printable: *hide_non_printable,
                            atime: *atime,
                            display_path: status.name.clone(),
                        };
                        print_file_entry(&status, &path, &client, &config).await?;
                    }
                    return Ok(());
                }

                // Regular listing
                let mut files = if *recursive {
                    list_files_recursive(&client, &path).await?
                } else {
                    client.list_status(&path).await?
                };

                if files.is_empty() {
                    return Ok(());
                }

                // Print header
                if !*recursive {
                    println!("Found {} items", files.len());
                }

                // Calculate column widths for formatting
                calculate_column_widths(&files, &client).await?;

                // For recursive mode: use depth-first order by default, but apply additional sorting if specified
                if *recursive {
                    // Check if any sorting options are specified
                    let has_sorting_options = *mtime || *size || *reverse || *atime;

                    if has_sorting_options {
                        // Apply sorting on top of depth-first order
                        sort_files(&mut files, *mtime, *size, *reverse, *atime);
                    }
                    // If no sorting options, files are already in depth-first order from list_files_recursive

                    // Print files
                    for file in &files {
                        let config = PrintConfig {
                            human_readable: *human_readable,
                            hide_non_printable: *hide_non_printable,
                            atime: *atime,
                            display_path: file.path.clone(),
                        };
                        print_file_entry(file, &CurvineURI::new(&file.path)?, &client, &config)
                            .await?;
                    }
                } else {
                    // Sort files based on options for non-recursive mode
                    sort_files(&mut files, *mtime, *size, *reverse, *atime);

                    // Print files
                    for file in &files {
                        let config = PrintConfig {
                            human_readable: *human_readable,
                            hide_non_printable: *hide_non_printable,
                            atime: *atime,
                            display_path: file.name.clone(),
                        };
                        print_file_entry(file, &CurvineURI::new(&file.path)?, &client, &config)
                            .await?;
                    }
                }

                Ok(())
            }
        }
    }
}

// Recursively list all files and directories using depth-first search
async fn list_files_recursive(
    client: &CurvineFileSystem,
    path: &CurvineURI,
) -> CommonResult<Vec<FileStatus>> {
    let mut all_files = Vec::new();

    // Use a stack to implement depth-first search
    // Each stack item contains (path, files_already_processed)
    let mut stack = vec![(path.clone(), false)];

    while let Some((current_path, files_processed)) = stack.pop() {
        if !files_processed {
            // First time visiting this path - get files and add them to result
            let files = client.list_status(&current_path).await?;

            // Sort files to ensure consistent order (directories first, then files)
            let mut sorted_files: Vec<_> = files.into_iter().collect();
            sorted_files.sort_by(|a, b| {
                // Directories first, then by name
                if a.is_dir != b.is_dir {
                    b.is_dir.cmp(&a.is_dir) // true (dir) comes before false (file)
                } else {
                    a.name.cmp(&b.name)
                }
            });

            // Add current directory's files to the result
            for file in &sorted_files {
                all_files.push(file.clone());
            }

            // Push this path back with files_processed = true
            stack.push((current_path, true));

            // Push subdirectories to the stack in reverse order for DFS
            for file in sorted_files.into_iter().rev() {
                if file.is_dir {
                    let child_path = CurvineURI::new(&file.path)?;
                    stack.push((child_path, false));
                }
            }
        }
        // If files_processed is true, we've already handled this path, so just continue
    }

    // Sort all files by path to ensure depth-first order
    all_files.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(all_files)
}

/// Recursively list all paths only using depth-first search
async fn list_paths_recursive(client: &CurvineFileSystem, path: &CurvineURI) -> CommonResult<()> {
    let mut to_process = vec![path.clone()];

    while let Some(current_path) = to_process.pop() {
        let files = client.list_status(&current_path).await?;

        // Sort files to ensure consistent order (directories first, then files)
        let mut sorted_files: Vec<_> = files.into_iter().collect();
        sorted_files.sort_by(|a, b| {
            // Directories first, then by name
            if a.is_dir != b.is_dir {
                b.is_dir.cmp(&a.is_dir) // true (dir) comes before false (file)
            } else {
                a.name.cmp(&b.name)
            }
        });

        // Process files in reverse order to maintain depth-first traversal
        for file in sorted_files.into_iter().rev() {
            println!("{}", file.path);

            if file.is_dir {
                let child_path = CurvineURI::new(&file.path)?;
                to_process.push(child_path);
            }
        }
    }

    Ok(())
}

/// Sort files based on the specified criteria
fn sort_files(files: &mut [FileStatus], mtime: bool, size: bool, reverse: bool, atime: bool) {
    files.sort_by(|a, b| {
        let cmp = if mtime {
            // Sort by modification time (most recent first)
            let a_time = if atime { a.atime } else { a.mtime };
            let b_time = if atime { b.atime } else { b.mtime };
            b_time.cmp(&a_time)
        } else if size {
            // Sort by size (largest first)
            b.len.cmp(&a.len)
        } else {
            // Sort by name
            a.name.cmp(&b.name)
        };

        if reverse {
            cmp.reverse()
        } else {
            cmp
        }
    });
}

/// Calculate column widths for formatting
async fn calculate_column_widths(
    _files: &[FileStatus],
    _client: &CurvineFileSystem,
) -> CommonResult<()> {
    // For now, we'll use fixed widths since we don't have dynamic content
    // In a full implementation, you would calculate max widths for each column
    Ok(())
}

/// Print a single file entry in HDFS-like format
async fn print_file_entry(
    file: &FileStatus,
    _path: &CurvineURI,
    _client: &CurvineFileSystem,
    config: &PrintConfig,
) -> CommonResult<()> {
    let file_type = if file.is_dir { "d" } else { "-" };
    let permissions = "rwxr-xr-x"; // Default permissions for now
    let replicas = if file.is_dir {
        "-"
    } else {
        &file.replicas.to_string()
    };
    let owner = "curvine"; // Default owner
    let group = "curvine"; // Default group

    // Format file size
    let size = if file.is_dir {
        "-".to_string()
    } else if config.human_readable {
        crate::cmds::fs::common::format_size(file.len as u64)
    } else {
        file.len.to_string()
    };

    // Format time (use access time if requested, otherwise modification time)
    let timestamp = if config.atime { file.atime } else { file.mtime };
    let datetime = chrono::DateTime::from_timestamp(timestamp / 1000, 0)
        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
    let formatted_time = datetime.format("%Y-%m-%d %H:%M").to_string();

    // Format filename (handle non-printable characters)
    let filename = if config.hide_non_printable {
        config
            .display_path
            .chars()
            .map(|c| {
                if c.is_ascii() && !c.is_ascii_control() {
                    c
                } else {
                    '?'
                }
            })
            .collect::<String>()
    } else {
        config.display_path.clone()
    };

    // Print in HDFS-like format
    println!(
        "{}{} {} {} {} {:>12} {} {}",
        file_type, permissions, replicas, owner, group, size, formatted_time, filename
    );

    Ok(())
}
