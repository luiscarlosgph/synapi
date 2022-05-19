"""
@brief   Module that contains basic functionalities to deal with a dataset
         stored in Synapse.
@author  Luis Carlos Garcia Peraza Herrera (luiscarlos.gph@gmail.com).
@date    11 May 2022.
"""

import synapseclient
import synapseutils
import pathlib
import tempfile
import os
import random
import string

class SynapseSession:
    def __init__(self, username: str, password: str, project_id: str):
        # Store params
        self.username = username
        self.password = password
        self.project_id = project_id
        
        # Connect to Synapse
        self.syn = synapseclient.Synapse()
        self.syn.login(self.username, self.password)

    def __del__(self):
        self.syn.logout()
    
    def get_id(self, path, parent_id=None, sep='/'):
        """
        @brief Get the synID of a Synapse file or folder.

        @param[in]  path           Relative path to the possible file or folder.
        @param[in]  parent_id      Synapse ID of the parent folder/project. 

        @returns the ID if it exists. Otherwise returns None. 
                 Synapse does not allow files and folders with the same name
                 to be stored in the same location. 
        """
        # The project is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id

        if path == '/':
            return parent_id
        elif sep in path:
            path_list = path.split(sep)
            child_id = self.syn.findEntityId(path_list[0], parent=parent_id)
            return self.get_id(sep.join(path_list[1:]), child_id)
        else: 
            return self.syn.findEntityId(path, parent=parent_id)
        
    def exists(self, path: str, concrete_type: list, parent_id=None):
        """
        @brief Checks whether a folder exists in the repository.

        @param[in]  path           Absolute path within the Synapse project.
        @param[in]  concrete_type  Synapse entity type.
        @param[in]  parent_id      Synapse ID of the parent folder/project.

        @returns True if the folder in path exists. Otherwise returns False.
        """
        # The project is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id
        
        fsynid = self.get_id(path, parent_id)
        if fsynid:
            info = self.syn.get(fsynid, downloadFile=False)
            return True if info.properties['concreteType'] in concrete_type else False
        else:
            return False

    def dir_exists(self, path: str, parent_id=None) -> bool:
        """
        @brief Checks whether a folder exists in the repository.

        @param[in]  path        Absolute path within the Synapse project.
        @param[in]  parent_id   Synapse ID of the parent folder/project.

        @returns True if the folder in path exists. Otherwise returns False.
        """
        if path and path[0] == '/':
            path = path[1:]
        return self.exists(path, ['org.sagebionetworks.repo.model.Folder', 
                                  'org.sagebionetworks.repo.model.Project'], 
                           parent_id)

    def file_exists(self, path: str, parent_id=None) -> bool:
        """
        @brief Discover whether a file exists in the repository.

        @param[in]  path        Absolute path within the Synapse project.
        @param[in]  parent_id   Synapse ID of the parent folder/project.

        @returns True if the file exists. Otherwise returns False.
        """
        if path and path[0] == '/':
            path = path[1:]
        return self.exists(path, ['org.sagebionetworks.repo.model.FileEntity'], 
                           parent_id)

    def upload(self, local_path: str, remote_path: str, parent_id=None,
               hidden: bool = False):
        """
        @brief    Upload a file or a directory tree to Synapse.
        @details  This method will overwrite whatever is already stored in the 
                  repository. Before uploading, you should check that the 
                  remote path does not exist.

        @param[in]  local_path   Path to the local file/folder.
        @param[in]  remote_path  Relative path (to the parent_id given) 
                                 pointing to the desired remote location.
                                 The containing folder of this remote path 
                                 must already exist in the repository.
        @param[in]  parent_id    Synapse ID of the parent folder/project.
        @param[in]  hidden       Flag to upload hidden files.
                                 False by default.
        """
        # The project is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id

        # Remove initial slash if present
        if remote_path and remote_path[0] == '/':
            remote_path = remote_path[1:]

        # Get id of the parent directory containing the remote path
        if '/' in remote_path:
            container_path = os.sep.join(remote_path.split(os.sep)[:-1])
            container_id = self.get_id(container_path, parent_id)
            # Make sure the destination directory exists
            if container_id is None:
                raise OSError('[ERROR] The remote directory ' \
                    + container_path \
                    + ' does not exist, so we cannot upload ' \
                    + local_path + ' to ' + remote_path)
        else:
            container_id = parent_id

        # Get just the name of the file/folder, without the rest of the path
        fname = os.path.basename(local_path)

        # Upload file
        if os.path.isfile(local_path):
            if hidden or not fname.startswith('.'):
                data = synapseclient.File(path=local_path, 
                                          name=os.path.basename(remote_path), 
                                          parent=container_id)
                data = self.syn.store(data)
                
        # Upload directory
        elif os.path.isdir(local_path):
            folder = synapseclient.Folder(name=os.path.basename(remote_path), 
                                          parent=container_id)
            folder = self.syn.store(folder)
            
            # Upload the children files and folders
            for f in os.listdir(local_path):
                self.upload(os.path.join(local_path, f), f, 
                            folder.properties.id, hidden)

    def download(self, remote_path: str, local_path, parent_id=None,
                 synapse_file_type: str = 'org.sagebionetworks.repo.model.FileEntity',
                 synapse_dir_type: str = 'org.sagebionetworks.repo.model.Folder'):
        """
        @param[in]  remote_path  Relative path (from parent_id) to a file or
                                 folder stored in Synapse.
        @param[in]  local_path   Path to the destination file/folder in the
                                 local filesystem. It must not exist.
        """
        # The project is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id

        # Remove initial slash if present
        if remote_path and remote_path[0] == '/':
            remote_path = remote_path[1:]

        # Check that the destination file/folder does not exist
        if os.path.exists(local_path):
            raise OSError('[ERROR] Cannot download ' \
                + remote_path + ' to ' + local_path \
                + ' because ' + local_path + ' already exists.')

        # Check that the parent directory of the local path exists
        container_path = pathlib.Path(local_path).parent.absolute()
        if not os.path.isdir(container_path):
            raise OSError('[ERROR] Cannot download ' \
                + remote_path + ' to ' + local_path \
                + ' because the parent of ' + local_path + ' does not exist.')
        
        # If the remote path points to a file
        if self.file_exists(remote_path, parent_id):
            entity = self.syn.get(self.get_id(remote_path, parent_id), 
                                  downloadFile=True,
                                  downloadLocation=tempfile.gettempdir(),
                                  ifcollision='overwrite.local')
            os.rename(entity['path'], local_path) 
        
        # If the remote path points to a folder
        elif self.dir_exists(remote_path, parent_id):
            # Create a directory in the local path with the same name
            os.mkdir(local_path)

            # Get synID of the remote directory
            remote_id = self.get_id(remote_path, parent_id)

            # List all the files and folders inside the remote folder
            children = self.syn.getChildren(remote_id, 
                                            includeTypes=['folder', 'file'])

            # Download all the children files and folders
            for child in children:
                self.download(child['name'], 
                              os.path.join(local_path, child['name']),
                              remote_id)

        else:
            raise ValueError('[ERROR] The remote path ' \
                + remote_path + ' does not point to a valid ' \
                + 'file or a folder in Synapse.')

    def mkdir(self, path: str, parent_id=None):
        """
        @brief  Creates a folder within the given folder/project.

        @param[in]  path        Relative path to the new folder. 
        @param[in]  parent_id   Synapse ID of the parent folder/project.

        @returns the syn ID of the new folder.
        """
        # The project is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id

        # Remove initial slash (if present)
        if path and path[0] == '/':
            path = path[1:]

        # If the last folder of the path exists, we should not be creating it
        if self.dir_exists(path, parent_id): 
            raise ValueError('[ERROR] Cannot create a folder that already exists.' \
                + ' There is already a ' + path + ' in ' + parent_id + '.')
       
        # Create all the folders of the provided path
        path_list = path.split(os.sep)
        child_id = None
        if self.dir_exists(path_list[0], parent_id):
            child_id = self.get_id(path_list[0], parent_id)
        else:
            # Create folder
            folder = synapseclient.Folder(path_list[0], parent_id)
            folder = self.syn.store(folder, createOrUpdate=False)
            child_id = folder.properties.id
        
        # Keep traversing the path if we have not finished yet, otherwise return 
        # the synID of the last (and new) folder
        if len(path_list) == 1:
            return child_id
        else:
            return self.mkdir(os.sep.join(path_list[1:]), child_id)

    def rm(self, path: str, parent_id=None):
        """
        @brief Delete a file or a folder.
         
        @param[in]  path        Relative path to the file or folder to be deleted.
        @param[in]  parent_id   Synapse ID of the parent folder/project.

        @returns nothing.
        """
        # The project is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id

        # Remove initial slash if present
        if path and path[0] == '/':
            path = path[1:]

        entity_id = self.get_id(path, parent_id)
        if entity_id is None:
            raise ValueError('[ERROR] The file in ' + path \
                + ' that you are trying to delete does not exist.')
        self.syn.delete(entity_id)

    def get_parent_id(self, path: str, parent_id=None) -> str:
        """
        @brief   Get the parent id of a Synapse object.
        @details The path does not need to exist, as we are only interested
                 to know if the parent folder of the path exists.

        @param[in]  path  Path whose parent synID we want to retrieve.

        @returs the parent synID of the object pointed by the given path.
                If the parent folder does not exist, None is returned.
        """
        # The project folder is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id
        
        if path == '/' and parent_id == self.project_id:
            raise ValueError('[ERROR] The project does not have a parent.')
        elif '/' in path:
            return self.get_id(os.sep.join(path.split(os.sep)[:-1]), parent_id)
        else:
            return parent_id

    def mv(self, src_path: str, dst_path: str, parent_id=None):   
        """
        @brief   Moves a file or folder from the src_path to the dst_path.
        @details The destination path must not exist.

        @param[in]  src_path   Remote relative path (from parent_id).
        @param[in]  dst_path   Remote relative path (from parent_id).
        @param[in]  parent_id  Synapse ID of the parent folder/project.

        @returns nothing.    
        """
        # The project is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id

        # Remove initial slash if present
        if dst_path and dst_path[0] == '/':
            dst_path = dst_path[1:]

        # Check the the source file/folder exists
        if self.get_id(src_path, parent_id) is None:
            raise ValueError('[ERROR] mv() source ' + src_path \
                + ' does not exist.')

        # Check that the destination file/folder does not exist, 
        # we do not want to overwrite it 
        if self.get_id(dst_path, parent_id) is not None:
            raise ValueError('[ERROR] mv() destination ' + dst_path \
                + ' already exists.')

        # Check that the parent of the destination file/folder exists
        dst_path_parent_id = self.get_parent_id(dst_path, parent_id)
        if dst_path_parent_id is None:
            raise ValueError('[ERROR] mv() cannot move ' + src_path \
                + ' into the path ' + dst_path \
                + ' because the parent folder of this destination path' \
                + ' does not exist.')

        # Move entity to the requested container folder
        src_id = self.get_id(src_path, parent_id)
        self.syn.move(src_id, dst_path_parent_id)

        # Rename the filename if requested
        dst_fname = os.path.basename(dst_path)
        e = self.syn.get(src_id, downloadFile=False)
        e.properties['name'] = dst_fname
        e = self.syn.store(e)

    def cp(self, src_path: str, dst_path: str, parent_id=None):
        """
        @brief   Copy a file or folder to another path in the Synapse repo.
        @details The destination path must not exist. 

        @param[in]  src_path   Remote relative path (from parent_id).
        @param[in]  dst_path   Remote relative path (from parent_id).
        @param[in]  parent_id  Synapse ID of the parent folder/project.
        
        @returns nothing.
        """
        # The project is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id

        # Remove initial slash if present
        if dst_path and dst_path[0] == '/':
            dst_path = dst_path[1:]

        # Check the the source file/folder exists
        if self.get_id(src_path, parent_id) is None:
            raise ValueError('[ERROR] mv() source ' + src_path \
                + ' does not exist.')
        
        # Check that the destination file/folder does not exist, 
        # we do not want to overwrite it 
        if self.get_id(dst_path, parent_id) is not None:
            raise ValueError('[ERROR] mv() destination ' + dst_path \
                + ' already exists.')

        # Check that the parent of the destination file/folder exists
        dst_path_parent_id = self.get_parent_id(dst_path, parent_id)
        if dst_path_parent_id is None:
            raise ValueError('[ERROR] mv() cannot move ' + src_path \
                + ' into the path ' + dst_path \
                + ' because the parent folder of this destination path' \
                + ' does not exist.')

        # Create temporary folder
        temp_dir_name = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
        self.mkdir(temp_dir_name, parent_id)
        
        # Copy entity to the temporary folder
        src_id = self.get_id(src_path, parent_id)
        synapseutils.copy(self.syn, src_id, self.get_id(temp_dir_name, parent_id))

        # Move entity from temporary folder to destination path
        temp_dir_id = self.get_id(temp_dir_name, parent_id)
        src_fname = os.path.basename(src_path)
        dst_fname = os.path.basename(dst_path)
        self.mv(src_fname, dst_fname, temp_dir_id)
        self.mv(os.path.join(temp_dir_name, dst_fname), dst_path)

        # Remove temporary folder
        self.rm(temp_dir_name, parent_id)

    def ls(self, remote_path: str, parent_id=None) -> list:
        """
        @brief List the contents of a directory in Synapse.

        @param[in]  remote_path  Relative path (from the parent_id) to the
                                 directory you want to list.
        @param[in]  parent_id    SynID of the directory that serves as a base
                                 for the path.

        @returns a list with the names of the file and folders.  
        """
        # The project is the parent if none is specified
        parent_id = self.project_id if parent_id is None else parent_id

        # Make sure that the directory exists
        if not self.dir_exists(remote_path, parent_id):
            raise OSError('[ERROR] The directory ' + remote_path \
                          + ' does not exist.')

        # Get the synID of the folder
        dir_id = self.get_id(remote_path, parent_id)

        # List all the files and folders inside the remote folder
        children = self.syn.getChildren(dir_id, includeTypes=['folder', 'file'])
        files = [x['name'] for x in children]
        
        return files


if __name__ == '__main__':
    raise RuntimeError('[ERROR] The synapi module cannot be executed as a script.')
