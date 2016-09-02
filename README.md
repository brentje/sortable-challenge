# sortable-challenge

MatchMaker
A solution to the Sortable.com coding challenge

Author: Brent Englehart

To Build
--------
To compile this program, run the Build command.  This only needs to be run once:

./build.sh

To Run
------	
To run the compiled version of this program, type:

./go.sh

Alternatively, you can run this program without building it first by typing:

scala MatchMaker.scala


--------
This program will accept 2 JSON files as input, "products.txt" and "listings.txt" which are located in the "data" folder.  

Input Files:

product.txt

{

"product_name": String // A unique id for the product

"manufacturer": String

"family": String // optional grouping of products

"model": String

"announced-date": String // ISO-8601 formatted date string, e.g. 2011-04-28T19:00:00.000-05:00

}


listing.txt

{

"title": String // description of product for sale

"manufacturer": String // who manufactures the product for sale

"currency": String // currency code, e.g. USD, CAD, GBP, etc.

"price": String // price, e.g. 19.99, 100.00

}

The files will be processed, and each record in products.txt will be searched for within the listings.txt file.  Multiple records, if available, will be pulled from listings.txt in order to create search results.

Results File:

A file named "results.txt", located in the "results" folder, will contain a list of all results found for each product record.  If a "results.txt" already exists, it will be overwritten.

results.txt

{

"product_name": String

"listings": Array[Listing]

}
