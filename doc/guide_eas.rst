.. _eas:

Downloading Data from the EAS
=============================

This guide outlines the fundamentals of how to find products in the EAS and how to query for them. For repeated use, it’s best to write a script to handle queries and modify it as necessary for specific runs. This has already been done for various products in the SHE_IAL_Pipelines project, in the `get_all_*_products.sh scripts <scripts.html#get-all-products-sh>`__, which can be used as a base for writing scripts for other products.

Identifying Products in the EAS
-------------------------------

You will first need to know which products you wish to download. Another PF might provide convenient links to their uploaded products, e.g.: https://euclid.roe.ac.uk/projects/vis_pf/wiki/Data_production_-_SC4#SC4-R9, from which you can get the required information. Otherwise, use the web view service https://eas-dps-cus.euclid.astro.rug.nl/DbView to search for the products you want, and then:

* Log in by clicking on the link next to “user” at the top
* Select the desired project (usually "test" for simulated data used for test runs such as Science Challenges)
* Click on “Tables”, select the appropriate category, then select the data product type
* Change search settings if desired to further limit the selection of products that will show up, then click “Submit” to find a list of all products
* Select the product(s) you wish to know about

You will now need a way to identify the set of products you wish to download based on information available in their ``.xml`` data products, and set up a query for them that can be used to retrieve them from the database.

If you wish to download one specific product, then find the ``ObjectId`` at ``Header`` -> ``ProductId`` -> ``ObjectId`` (NOT the ``object_id``). The query you’ll use will then be ``Header.ProductId.ObjectId=<ObjectId>``

You might alternatively wish to download many products. There are many ways a set of products can be specified. For instance, often another PF might identify a set of products with the ``Header.DataSetRelease`` attribute, so your query will be something like ``Header.DataSetRelease=<DataSetRelease>``. Queries can be combined with ``&&``, e.g. ``Header.DataSetRelease=<DataSetRelease>&&Data.ObservationSequence.ObservationId==<ObservationId>``.

Downloading Products and Data
-----------------------------

To download products and data, you’ll want to use the data product retrieval script, which is available at https://euclid.roe.ac.uk/attachments/download/26136/dataProductRetrieval_SC8.py. Put this file in some logical place (e.g. your bin directory, but be careful to run it with python and not try to execute it directly or it’ll do weird things to your mouse cursor. Seriously.) You can then download products and data with a command such as:
``python ~/bin/dataProductRetrieval_SC8.py --username <ESAC username> --password <ESAC password> --project TEST --data_product <DataProductType> --query "<query>"``

To keep your password secure, you can point the ``--password`` argument to a file which contains your password and then delete the file afterward, which is a bit more secure than leaving it in your bash history.

This script will only re-download fits files if they don’t already exist, so it can be interrupted and re-run without worry about completely starting over. However, if a datafile is partially downloaded and the download is interrupted, it won’t realize this. If this happens, you’ll need to delete it and start the download again.
