.. _webdav:

Using the SDC-UK WebDAV Server
==============================

The SDC-UK WebDAV server is an online file store for storing and sharing small amounts of test data (<500GB per user) used by pipelines and developers.  It is hosted at SDC-UK, and is open to SDC-UK members and OU-SHE developers.  The service authenticates against the ESA LDAP directory, so users must be officially registered there before gaining access. A read-only username and password is also available for those who do not require write access:

**Username:** ou-she

**Password:** testDATAsdcUK

Web GUI
-------

Files can be browsed and downloaded through a web browser by going to https://sdcuk-webdav.roe.ac.uk/ and logging in.

Mounting WebDAV for read/write
------------------------------

To mount the WebDAV service on your own Linux machine from the command line, follow these steps:

#. Install the DAVfs package (CentOS - ``sudo yum install davfs2``, Ubuntu - ``apt-get install davfs2``)
#. Make a directory for the mount - ``mkdir -p /mnt/webdav``
#. Run the mount command as root - ``sudo mount -t davfs https://sdcuk-webdav.roe.ac.uk/ /mnt/webdav``
#. Enter your root password, then your username and password when prompted. Either your ESAC/LDAP credentials or the read-only username and password provided above can be used.

Done!

Usage Notes
-----------

As the WebDAV service needs to be mounted as root, any writes to it will similarly need to be performed as root.

In order to ensure readability, please don't put your data into the root directory, instead follow these guidelines:

* For random data you wish to share with others, create a folder named the same as your ESA username
* For data used in benchmarking or integration testing, create a folder names the same as the gitlab project that will utilize that data

Note that all data hosted on the service is visible to anyone with a valid ESA LDAP account or knowledge of the read-only username and password.  Also note that all users have read/write access to the entire directory, and hence can add or delete data regardless of ownership.  Finally, note that none of the data hosted on this service is backed up.

Using WebDAV with CODEEN
------------------------

For instructions on how to use the WebDAV service within CODEEN as a source for test data, follow the guidelines here: https://euclid.roe.ac.uk/projects/testdata/wiki/Synchronization_tool_as_an_Elements_service.

For this server, you can set up a ``sync.conf`` file with contents such as the following:

.. code:: text

    overwrite = false
    host = WebDAV
    user = ou-she
    password = testDATAsdcUK
    host-url = https://sdcuk-webdav.roe.ac.uk
    distant-workspace = /PF-SHE
    local-workspace = /tmp
    tries = 8
