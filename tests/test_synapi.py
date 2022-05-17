"""
@brief  Unit tests to check the Synapse module.
@author Luis Carlos Garcia Peraza Herrera (luiscarlos.gph@gmail.com).
@date   11 May 2022.
"""
import unittest
import tempfile
import random
import os
import time
import shutil
import synapseclient

# My imports
import synapi

class TestSynapseMethods(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        # Read Synapse credentials
        self.username = None
        self.password = None
        self.project_id = None

        super(TestSynapseMethods, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        # Get the Synapse credentials to access the repo 
        cls._username = input('Username: ')
        cls._password = input('Password: ')
        cls._project_id = input('Project id: ')

    def test_file_upload_and_download(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Create temporary file
        upload_fname = 'test_synapse_methods_upload.txt'
        download_fname = 'test_synapse_methods_download.txt'
        remote_fname = 'test_synapse_methods_remote.txt'
        upload_path = os.path.join(tempfile.gettempdir(), upload_fname) 
        download_path = os.path.join(tempfile.gettempdir(), download_fname)
        content = 'Testing SynapseSession upload and download.'
        with open(upload_path, 'w') as f:
            f.write(content)

        # Upload file to Synapse
        sess.upload(upload_path, remote_fname)
        os.unlink(upload_path)

        # Download file from Synapse
        sess.download(remote_fname, download_path)
        
        # Make sure the file was downloaded
        self.assertTrue(os.path.isfile(download_path))
        
        # Read the contents of the downloaded file
        with open(download_path, 'r') as f:
            lines = f.readlines()
        os.unlink(download_path)

        # Check that the uploaded and downloaded files are the same
        self.assertEqual(content, lines[0])

        # Remove test file from repo
        sess.rm(remote_fname)

    def test_folder_upload_and_download(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Create a folder with a file inside it
        folder_path = os.path.join(tempfile.gettempdir(), 'dummy_folder') 
        file_path = os.path.join(folder_path, 'dummy_file.txt')
        if os.path.isdir(folder_path):
            shutil.rmtree(folder_path)
        os.mkdir(folder_path)
        content = 'Testing SynapseSession upload and download.'
        with open(file_path, 'w') as f:
            f.write(content)

        # Upload folder to Synapse
        remote_fname = 'dummy_remote_folder'
        sess.upload(folder_path, remote_fname)
        shutil.rmtree(folder_path)

        # Download folder from Synapse
        download_fname = 'dummy_downloaded_folder'
        download_path = os.path.join(tempfile.gettempdir(), download_fname)
        sess.download(remote_fname, download_path)

        # Check that the folder has been downloaded and contains the file
        self.assertTrue(os.path.isdir(download_path))
        self.assertTrue(os.path.isfile(os.path.join(download_path, 'dummy_file.txt')))

        # Check the contents of the file
        with open(os.path.join(download_path, 'dummy_file.txt'), 'r') as f:
            lines = f.readlines()
        shutil.rmtree(download_path)
        self.assertEqual(content, lines[0])

        # Remove folder from Synapse
        sess.rm(remote_fname)

    def test_mkdir(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Create remote folder
        remote_path = 'foo1/foo2/foo3/foo4/foo5'
        sess.mkdir(remote_path)

        # Check that the folder was successfully created
        self.assertTrue(sess.dir_exists(remote_path))

        # Remove remote folder
        sess.rm(remote_path)

        # Check that the folder no longer exists
        self.assertFalse(sess.dir_exists(remote_path))

        # Remove the whole tree we created
        sess.rm('foo1')
        
    def test_get_file_id(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)
        
        # Write dummy file to disk
        file_path = os.path.join(tempfile.gettempdir(), 'superfoo.txt') 
        content = 'Testing SynapseSession upload and download.'
        with open(file_path, 'w') as f:
            f.write(content)

        # Upload the file to Synapse using the Synapse API so we get the synID
        data = synapseclient.File(path=file_path,
                                  parent=TestSynapseMethods._project_id)
        data = sess.syn.store(data)
        gt_id = data.properties['id']

        # Get the synID using our API
        pred_id = sess.get_id('superfoo.txt')
        
        # Check that the ids match
        self.assertEqual(pred_id, gt_id)

        # Remove the file from the remote repository
        sess.rm('superfoo.txt')

    def test_get_dir_id(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)
 
        # Create a remote dir
        folder_name = 'foo_test_dir'
        folder = synapseclient.Folder(name=folder_name,
                                      parent=TestSynapseMethods._project_id)
        folder = sess.syn.store(folder)

        # Check we are able to retrieve the synID correctly 
        self.assertEqual(folder.properties['id'], sess.get_id(folder_name))

        # Remove the directory from the remote repository
        sess.rm(folder_name)

    def test_file_exists(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Check that file does not exist
        fname = 'unit_test_file_exists.txt'
        self.assertFalse(sess.file_exists(fname))

        # Write dummy file to disk
        file_path = os.path.join(tempfile.gettempdir(), fname) 
        content = 'Testing SynapseSession file exists method.'
        with open(file_path, 'w') as f:
            f.write(content)

        # Upload dummy file
        sess.upload(file_path, fname)
        os.unlink(file_path)

        # Check that file now exists
        self.assertTrue(sess.file_exists(fname))

        # Delte dummy file
        sess.rm(fname)

    def test_file_rm(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Write dummy file to disk
        fname = 'unit_test_file_exists.txt'
        file_path = os.path.join(tempfile.gettempdir(), fname) 
        content = 'Testing SynapseSession file remove method.'
        with open(file_path, 'w') as f:
            f.write(content)

        # Upload file to remote repo
        sess.upload(file_path, fname)
        os.unlink(file_path)

        # Delete file
        sess.rm(fname)

        # Make sure it no longer exists
        self.assertFalse(sess.file_exists(fname))

    def test_dir_rm(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Make sure the directory does not exist 
        dirname = 'test_dir_rm'
        self.assertFalse(sess.dir_exists(dirname))

        # Create directory in Synapse
        sess.mkdir(dirname)

        # Make sure the directory now exists
        self.assertTrue(sess.dir_exists(dirname))

        # Remove remote dummy dir 
        sess.rm(dirname)

        # Make sure the directory no longer exists
        self.assertFalse(sess.dir_exists(dirname))

    def test_file_mv(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Create dummy file
        fname = 'unit_test_file_mv.txt'
        file_path = os.path.join(tempfile.gettempdir(), fname)
        content = 'Testing SynapseSession file move method.'
        with open(file_path, 'w') as f:
            f.write(content)

        # Upload dummy file
        sess.upload(file_path, fname)
        os.unlink(file_path)
        
        self.assertTrue(sess.file_exists(fname))

        # Move dummy file
        new_fname = 'this_is_the_new_file.txt'
        sess.mv(fname, new_fname)

        self.assertFalse(sess.file_exists(fname))
        self.assertTrue(sess.file_exists(new_fname))

        # Remove dummy file
        sess.rm(new_fname)

    def test_dir_mv(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Create remote directory tree
        sess.mkdir('superfoo1/superfoo2/superfoo3')

        # Create remote directory
        sess.mkdir('superdestfoo')

        # Make sure that the source folder exists
        self.assertTrue(sess.dir_exists('superfoo1/superfoo2/superfoo3'))
        self.assertTrue(sess.dir_exists('superdestfoo'))

        # Move directory
        sess.mv('superfoo1', 'superdestfoo/newname')

        # Make sure that the destination folder exists
        self.assertTrue(sess.dir_exists('superdestfoo/newname'))
        self.assertTrue(sess.dir_exists('superdestfoo/newname/superfoo2'))
        self.assertTrue(sess.dir_exists('superdestfoo/newname/superfoo2/superfoo3'))

        # Make sure that the source folder does not exist
        self.assertFalse(sess.dir_exists('superfoo1'))

        # Remove test folders
        sess.rm('superdestfoo')

    def test_file_cp(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Create dummy file
        fname = 'test_file_cp.txt'
        file_path = os.path.join(tempfile.gettempdir(), fname)
        content = 'Testing SynapseSession file copy method.'
        with open(file_path, 'w') as f:
            f.write(content)

        # Upload dummy file
        sess.upload(file_path, fname)

        # Delete local copy of the dummy file
        os.unlink(file_path)

        # Copy the dummy file in the repo
        new_fname = 'new_fname_test_file_cp.txt'
        sess.cp(fname, new_fname)

        # Check that the copy exists
        self.assertTrue(sess.file_exists(new_fname))

        # Download the copy
        sess.download(new_fname, file_path)

        # Check that it has the same contents
        with open(file_path, 'r') as f:
            lines = f.readlines()
        self.assertEqual(content, lines[0])

        # Remove the downloaded file (local)
        os.unlink(file_path)

        # Remove both remote files
        sess.rm(fname)
        sess.rm(new_fname)

    def test_dir_cp(self):
        # Log into Synapse
        sess = synapi.SynapseSession(TestSynapseMethods._username,
                                           TestSynapseMethods._password,
                                           TestSynapseMethods._project_id)

        # Create a directory
        sess.mkdir('dir_cp_foo1/dir_cp_foo2')
        self.assertTrue(sess.dir_exists('dir_cp_foo1/dir_cp_foo2'))

        # Copy directory
        sess.cp('dir_cp_foo1', 'dir_cp_foo3')
        
        # Check that it was copied
        self.assertTrue(sess.dir_exists('dir_cp_foo3'))
        self.assertTrue(sess.dir_exists('dir_cp_foo3/dir_cp_foo2'))

        # Remove unit test dirs
        sess.rm('dir_cp_foo1')
        sess.rm('dir_cp_foo3')


if __name__ == '__main__':
    unittest.main()
