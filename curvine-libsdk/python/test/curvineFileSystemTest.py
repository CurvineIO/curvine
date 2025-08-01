import unittest
from curvinefs.curvineFileSystem import CurvineFileSystem

class Test(unittest.TestCase):
    # Test file system function
    def test(self):
        conf_path = "D:/Users/S9059520/Curvine/testing/curvine-cluster.toml"
        fs = CurvineFileSystem(conf_path, 128, 8)
        test_path = "file:///fs_test"

        fs.mkdir(test_path, True)

        # fs.rm(test_path, False)

        file_status = fs.get_file_status(test_path)
        print("File(Directory) status:",file_status)
        print("------------------------------------------------")

        none_file_status = fs.get_file_status(test_path+"/f.txt")
        print("ls None file:",none_file_status)
        print("------------------------------------------------")

        
        master_status = fs.get_master_info()
        print("Master information:",master_status)
        print("------------------------------------------------")

        writer = fs.create(test_path+"/a.txt", True)
        bytes_data = bytes("ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ",'utf-8')
        writer.write(bytes_data)
        writer.close()

        writer = fs.create(test_path+"/b.txt", True)
        bytes_data = bytes("ABCDEFGHIJKLMNOPQRSTUVWXYZ",'utf-8')
        writer.write(bytes_data)
        writer.close()   

        writer = fs.append(test_path+"/b.txt")
        bytes_data = bytes("1234567890",'utf-8')
        writer.write(bytes_data)
        writer.close()

        file_status = fs.get_file_status(test_path)
        print("File status:",file_status)
        print("------------------------------------------------")

        is_dir = fs.isdir(test_path)
        print("Test path is dir:", is_dir)
        print("------------------------------------------------")

        is_dir = fs.isdir(test_path+"/a.txt")
        print("File path is dir:", is_dir)
        print("------------------------------------------------")

        is_file = fs.isfile(test_path+"/a.txt")
        print("File path is file:", is_file)
        print("------------------------------------------------")

        reader = fs.open(test_path+"/a.txt")
        data = reader.read(0,3)
        print("read data:", data)
        print("------------------------------------------------")

        reader.seek(6)
        data = reader.read(0,4)
        print("read data after seek:", data)
        print("------------------------------------------------")
        reader.close()

        ls = fs.ls(test_path, False)
        print("ls result:",ls)
        print("------------------------------------------------")

        ls_detailed = fs.ls(test_path, True)
        print("ls detailed result:",ls_detailed)
        print("------------------------------------------------")

        cat_result = fs.cat_file(test_path+"/a.txt")
        print("cat file result:", cat_result)
        print("------------------------------------------------")

        cat_result = fs.cat(test_path)
        print("cat file result:", cat_result)
        print("------------------------------------------------")

        data = fs.head(test_path+"/a.txt",8)
        print("read head:", data)
        print("------------------------------------------------")

        data = fs.tail(test_path+"/a.txt",3)
        print("read tail:", data)
        print("------------------------------------------------")

        fs.mv(test_path+"/a.txt",test_path+"/e.txt")
        ls = fs.ls(test_path, False)
        print("ls result after mv:",ls)
        print("------------------------------------------------")
        fs.mv(test_path+"/e.txt",test_path+"/a.txt")
        
        fs.touch(test_path+"/a.txt", True)
        ls_detailed = fs.ls(test_path, True)
        print("ls result after touch true:",ls_detailed)
        print("------------------------------------------------")
 
        fs.touch(test_path+"/f.txt", True)
        ls_detailed = fs.ls(test_path, True)
        print("ls result after touch empty file:",ls_detailed)
        print("------------------------------------------------")

        fs.rename(test_path+"/a.txt",test_path+"/aa.txt")
        ls = fs.ls(test_path, False)
        print("ls result after rename:",ls)
        print("------------------------------------------------")   
        fs.rename(test_path+"/aa.txt",test_path+"/a.txt")

        fs.close()


# python test/curvineFileSystemTest.py
if __name__ == '__main__':
    suite = unittest.TestSuite()  
    suite.addTest(Test("test"))  
    runner = unittest.TextTestRunner()  
    runner.run(suite)  

