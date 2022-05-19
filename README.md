Description
-----------
Python package that will allow you to treat the Synapse repository as a local directory.

To use this package you need to be a Synapse Certified User. To get certified you have to go to your *Account Settings*, click *Get Certified*, and pass the quiz you will be prompted with.

Install with pip
----------------
```
$ python3 -m pip install synapi --user
```

Install from source
-------------------
```
$ python3 setup.py install --user
```

Run unit tests
--------------
```
$ python3 tests/test_synapi.py
```
This script will prompt you for your Synapse credentials and the ID of a Synapse project where the tests will be executed.

Exemplary code snippet
----------------------
```
#!/usr/bin/env python3

import synapi

# Login into Synapse
sess = synapi.SynapseSession('username', 'password', 'project_id')       

# Upload a file or folder
sess.upload('local/path', 'remote/path')

# Download a file or folder
sess.download('remote/path', 'local/path')

# Make a directory
sess.mkdir('remote/path')

# Check whether a file/folder exists
sess.file_exists('remote/path')
sess.dir_exists('remote/path')

# Copy file or directory
sess.cp('remote/path1', 'remote/path2')

# Move file or directory
sess.mv('remote/path1', 'remote/path2')

# Remove file or directory
sess.rm('remote/path')

# List a project or directory in Synapse
files = sess.ls('remote/path')

# Get the Synapse ID of a file or folder
synapse_id = sess.get_id('remote/path')

```

All methods have a `parent_id` parameter, if you do not specify one, the parent ID used is the project ID passed in the constuctor.
