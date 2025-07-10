use crate::{FilesystemConf, LibFilesystem, LibFsReader, LibFsWriter};
use bytes::BytesMut;
use curvine_common::FsResult;
use pyo3::prelude::*; 


//Define the PythonFilesystem struct
#[pyclass]
pub struct PythonFilesystem {
    inner: LibFilesystem,
}

//Implement methods for PythonFilesystem
impl  PythonFilesystem{
    //New PythonFilesystem 
    pub fn new(conf: String) -> FsResult<Self>{
        let fs_conf = FilesystemConf::from_str(conf)?;
        let cluster_conf = fs_conf.into_cluster_conf()?;

        let inner = LibFilesystem::new(cluster_conf)?;
        Ok(Self{inner})
    } 

    //Create file
    pub fn create(
        &self, 
        path: String,
        overwrite: bool,
    ) -> FsResult<LibFsWriter>{
        self.inner.create(path, overwrite)
    } 

    //Append content
    pub fn append(&self, path: String) -> FsResult<LibFsWriter>{
        self.inner.append(path)
    }

    //Create a directory
    pub fn mkdir(
        &self,
        path: String,
        create_parent: bool,
    ) -> FsResult<bool>{
        self.inner
            .mkdir(path, create_parent)
    }

    //Open file
    pub fn open(&self, path: String) -> FsResult<LibFsReader>{
        self.inner.open(path)
    }

    //Get file status
    pub fn get_file_status(&self, path: String) -> FsResult<BytesMut>{ //之后要考虑这样传递（直接返回所有权）有没有问题
        let status = self.inner.get_status(path)?;
        Ok(status)

    }

    //List file system
    pub fn list_status(&self, path: String) -> FsResult<BytesMut>{
        let status = self.inner.list_status(path)?;
        Ok(status)
    }

    //Rename file
    pub fn rename(&self, src: String, dst: String) -> FsResult<bool>{
        self.inner.rename(src,dst)
    }

    //Delete file
    pub fn delete(&self, path: String, recursive: bool) -> FsResult<()>{
        self.inner.delete(path, recursive)
    }

    //Get master information
    pub fn get_master_info(&self) -> FsResult<BytesMut>{
        let status = self.inner.get_master_info()?;
        Ok(status)
    }
}


